// Translation of the filters DuckDB pushes to a read_hudi scan into the
// kernel's `column op literal` predicates, for DuckDB 2.0.
//
// In 2.0 the comparison, BETWEEN and conjunction expressions are no longer
// dedicated classes: `=`/`<`/.../`!=` and BETWEEN are function expressions
// (ExpressionClass::BOUND_FUNCTION, recognised by their ExpressionType and by
// BoundComparisonExpression / BoundBetweenExpression helper accessors) and
// AND/OR remain BoundConjunctionExpression. `pushdown_complex_filter` still
// receives the filters as a flat list of single predicates (the optimizer's
// filter combiner has already split AND), but this file recurses into AND
// itself so a plain `SELECT ... WHERE a=1 AND b=2` still prunes.
//
// Every translated filter also stays in DuckDB's plan, so a filter this file
// skips only costs pruning. The one way to return wrong rows is a literal that
// does not round-trip through the kernel's string cast, so only types whose
// text form is exact are pushed: integers, strings, booleans, dates and
// decimals at the column's scale, and only when the literal already has the
// column's type. DuckDB's expression rewriter has removed every invertible
// cast by the time filters reach the scan, so a cast still wrapping a column
// is one DuckDB judged unsafe to undo and is not translated.

#include "hudi_scan.hpp"

#include "duckdb/planner/expression/bound_between_expression.hpp"
#include "duckdb/planner/expression/bound_columnref_expression.hpp"
#include "duckdb/planner/expression/bound_comparison_expression.hpp"
#include "duckdb/planner/expression/bound_conjunction_expression.hpp"
#include "duckdb/planner/expression/bound_constant_expression.hpp"
#include "duckdb/planner/expression/bound_function_expression.hpp"
#include "duckdb/planner/expression/bound_operator_expression.hpp"
#include "duckdb/planner/operator/logical_get.hpp"

namespace duckdb {

namespace {

struct FilterColumn {
	string name;
	LogicalType type;
};

class FilterTranslator {
public:
	FilterTranslator(const LogicalGet &get, const vector<Identifier> &names, const vector<LogicalType> &types)
	    : get(get), names(names), types(types) {
	}

	void Translate(const Expression &expr, vector<HudiFilterSpec> &out);

private:
	bool TryTranslateComparison(const Expression &expr, vector<HudiFilterSpec> &out);
	// The 2.0 in-clause rewriter turns `col NOT IN (v1, ..., vn)` into
	// `NOT (col = v1 OR ... OR col = vn)` before filters reach the scan, so
	// the NOT-IN operator never survives to pushdown. Recognise that exact
	// shape and emit NOT IN (a bare `col = v1 OR ...` without the NOT
	// wrapper is not recognised, matching the pre-2.0 behaviour where only
	// actual IN-lists were pushed).
	bool TryTranslateNegatedOr(const Expression &expr, vector<HudiFilterSpec> &out);
	bool TryTranslateNotIn(const Expression &expr, vector<HudiFilterSpec> &out);
	bool ResolveColumn(const Expression &expr, FilterColumn &column);
	bool ResolveComparison(const Expression &lhs, const Expression &rhs, const string &op, HudiFilterSpec &spec);
	bool ResolveLiteral(const Expression &expr, const FilterColumn &column, string &out);
	static bool RenderLiteral(const Value &value, string &out);
	static bool PushableType(const LogicalType &type);
	static const char *ComparisonOperator(ExpressionType type);

	const LogicalGet &get;
	const vector<Identifier> &names;
	const vector<LogicalType> &types;
};

bool FilterTranslator::PushableType(const LogicalType &type) {
	switch (type.id()) {
	case LogicalTypeId::BOOLEAN:
	case LogicalTypeId::TINYINT:
	case LogicalTypeId::SMALLINT:
	case LogicalTypeId::INTEGER:
	case LogicalTypeId::BIGINT:
	case LogicalTypeId::VARCHAR:
	case LogicalTypeId::DATE:
	case LogicalTypeId::DECIMAL:
		return true;
	default:
		return false;
	}
}

// A column reference of this scan.
bool FilterTranslator::ResolveColumn(const Expression &expr, FilterColumn &column) {
	if (expr.GetExpressionClass() != ExpressionClass::BOUND_COLUMN_REF) {
		return false;
	}
	auto &ref = expr.Cast<BoundColumnRefExpression>();
	auto &column_ids = get.GetColumnIds();
	auto &binding = ref.Binding();
	if (binding.table_index != get.table_index || binding.column_index >= column_ids.size()) {
		return false;
	}
	auto &column_index = column_ids[binding.column_index];
	if (column_index.HasChildren() || column_index.IsVirtualColumn()) {
		return false;
	}
	auto primary = column_index.GetPrimaryIndex();
	if (primary >= names.size()) {
		return false;
	}
	column.name = names[primary].GetIdentifierName();
	column.type = types[primary];
	return PushableType(column.type);
}

bool FilterTranslator::RenderLiteral(const Value &value, string &out) {
	if (value.IsNull()) {
		return false;
	}
	switch (value.type().id()) {
	case LogicalTypeId::BOOLEAN:
		out = BooleanValue::Get(value) ? "true" : "false";
		return true;
	case LogicalTypeId::TINYINT:
	case LogicalTypeId::SMALLINT:
	case LogicalTypeId::INTEGER:
	case LogicalTypeId::BIGINT:
	case LogicalTypeId::DECIMAL:
		out = value.ToString();
		return true;
	case LogicalTypeId::VARCHAR:
		out = StringValue::Get(value);
		return true;
	case LogicalTypeId::DATE: {
		auto date = DateValue::Get(value);
		if (!date.IsFinite()) {
			return false;
		}
		int32_t year, month, day;
		Date::Convert(date, year, month, day);
		if (year < 1 || year > 9999) {
			return false;
		}
		out = Date::ToString(date);
		return true;
	}
	default:
		return false;
	}
}

// The literal `expr` compared with the column, as the text the kernel casts
// back to the column's type; only a literal of exactly the column's type.
bool FilterTranslator::ResolveLiteral(const Expression &expr, const FilterColumn &column, string &out) {
	if (expr.GetExpressionClass() != ExpressionClass::BOUND_CONSTANT) {
		return false;
	}
	auto &value = expr.Cast<BoundConstantExpression>().GetValue();
	if (value.IsNull() || value.type() != column.type) {
		return false;
	}
	return RenderLiteral(value, out);
}

static const string *FlipOperator(const string &op) {
	static const string LT = "<", LTE = "<=", GT = ">", GTE = ">=", EQ = "=", NE = "!=";
	if (op == LT) {
		return &GT;
	}
	if (op == LTE) {
		return &GTE;
	}
	if (op == GT) {
		return &LT;
	}
	if (op == GTE) {
		return &LTE;
	}
	if (op == EQ) {
		return &EQ;
	}
	if (op == NE) {
		return &NE;
	}
	return nullptr;
}

bool FilterTranslator::ResolveComparison(const Expression &lhs, const Expression &rhs, const string &op,
                                         HudiFilterSpec &spec) {
	FilterColumn column;
	string literal;
	if (ResolveColumn(lhs, column) && ResolveLiteral(rhs, column, literal)) {
		spec.column = column.name;
		spec.op = op;
		spec.values = {literal};
		return true;
	}
	auto flipped = FlipOperator(op);
	if (flipped && ResolveColumn(rhs, column) && ResolveLiteral(lhs, column, literal)) {
		spec.column = column.name;
		spec.op = *flipped;
		spec.values = {literal};
		return true;
	}
	return false;
}

const char *FilterTranslator::ComparisonOperator(ExpressionType type) {
	switch (type) {
	case ExpressionType::COMPARE_EQUAL:
		return "=";
	case ExpressionType::COMPARE_NOTEQUAL:
		return "!=";
	case ExpressionType::COMPARE_LESSTHAN:
		return "<";
	case ExpressionType::COMPARE_LESSTHANOREQUALTO:
		return "<=";
	case ExpressionType::COMPARE_GREATERTHAN:
		return ">";
	case ExpressionType::COMPARE_GREATERTHANOREQUALTO:
		return ">=";
	default:
		return nullptr;
	}
}

// An 8-way comparison, which in 2.0 is a two-argument function expression
// (see BoundComparisonExpression).
bool FilterTranslator::TryTranslateComparison(const Expression &expr, vector<HudiFilterSpec> &out) {
	if (!BoundComparisonExpression::IsComparison(expr)) {
		return false;
	}
	auto &comparison = expr.Cast<BoundFunctionExpression>();
	auto op = ComparisonOperator(expr.GetExpressionType());
	if (!op) {
		return false;
	}
	HudiFilterSpec spec;
	if (ResolveComparison(BoundComparisonExpression::Left(comparison), BoundComparisonExpression::Right(comparison), op, spec)) {
		out.push_back(std::move(spec));
	}
	return true;
}

// 2.0 delivers `col NOT IN (a, b)` as NOT(IN(col, a, b)) — a NOT (OPERATOR_NOT)
// wrapping a COMPARE_IN operator whose children are [column, lit, lit, ...].
// (1.x's in-clause rewriter produced NOT(col=a OR col=b); the 2.0 in-clause
// simplification rule does not De Morgan it, so the IN operator survives to
// pushdown.) Translate that shape straight to NOT IN.
bool FilterTranslator::TryTranslateNotIn(const Expression &expr, vector<HudiFilterSpec> &out) {
	auto &not_op = expr.Cast<BoundOperatorExpression>();
	if (not_op.GetExpressionType() != ExpressionType::OPERATOR_NOT || not_op.GetChildren().size() != 1) {
		return false;
	}
	auto &inner = *not_op.GetChildren()[0];
	if (inner.GetExpressionClass() != ExpressionClass::BOUND_OPERATOR ||
	    inner.GetExpressionType() != ExpressionType::COMPARE_IN) {
		return false;
	}
	auto &in_op = inner.Cast<BoundOperatorExpression>();
	auto &children = in_op.GetChildren();
	if (children.size() < 2) {
		return false;
	}
	FilterColumn column;
	if (!ResolveColumn(*children[0], column)) {
		return false;
	}
	HudiFilterSpec spec;
	spec.column = column.name;
	spec.op = "NOT IN";
	for (idx_t i = 1; i < children.size(); i++) {
		string literal;
		if (!ResolveLiteral(*children[i], column, literal)) {
			return false;
		}
		spec.values.push_back(std::move(literal));
	}
	out.push_back(std::move(spec));
	return true;
}

bool FilterTranslator::TryTranslateNegatedOr(const Expression &expr, vector<HudiFilterSpec> &out) {
	auto &not_op = expr.Cast<BoundOperatorExpression>();
	if (not_op.GetExpressionType() != ExpressionType::OPERATOR_NOT || not_op.GetChildren().size() != 1) {
		return false;
	}
	auto &inner = *not_op.GetChildren()[0];
	if (inner.GetExpressionClass() != ExpressionClass::BOUND_CONJUNCTION ||
	    inner.GetExpressionType() != ExpressionType::CONJUNCTION_OR) {
		return false;
	}
	auto &or_expr = inner.Cast<BoundConjunctionExpression>();
	auto &disjuncts = or_expr.GetChildren();
	if (disjuncts.empty()) {
		return false;
	}
	// Every disjunct must be `col = literal` on the same column; collect the
	// literals first, only emit once they all resolve.
	string column_name;
	vector<string> literals;
	for (auto &child : disjuncts) {
		if (!BoundComparisonExpression::IsComparison(*child) ||
		    child->GetExpressionType() != ExpressionType::COMPARE_EQUAL) {
			return false;
		}
		auto &comparison = child->Cast<BoundFunctionExpression>();
		HudiFilterSpec check;
		if (!ResolveComparison(BoundComparisonExpression::Left(comparison), BoundComparisonExpression::Right(comparison), "=",
		                       check)) {
			return false;
		}
		if (!column_name.empty() && check.column != column_name) {
			return false;
		}
		column_name = check.column;
		literals.push_back(check.values.front());
	}
	HudiFilterSpec spec;
	spec.column = std::move(column_name);
	spec.op = "NOT IN";
	spec.values = std::move(literals);
	out.push_back(std::move(spec));
	return true;
}

void FilterTranslator::Translate(const Expression &expr, vector<HudiFilterSpec> &out) {
	switch (expr.GetExpressionClass()) {
	case ExpressionClass::BOUND_CONJUNCTION: {
		auto &conjunction = expr.Cast<BoundConjunctionExpression>();
		if (conjunction.GetExpressionType() != ExpressionType::CONJUNCTION_AND) {
			return;
		}
		for (auto &child : conjunction.GetChildren()) {
			Translate(*child, out);
		}
		return;
	}
	case ExpressionClass::BOUND_FUNCTION: {
		// In 2.0 comparisons and BETWEEN are function expressions, so the
		// function class carries all three shapes: compare, between, or an
		// ordinary function (which is not a prunable filter).
		if (TryTranslateComparison(expr, out)) {
			return;
		}
		auto &function = expr.Cast<BoundFunctionExpression>();
		if (function.GetExpressionType() == ExpressionType::COMPARE_BETWEEN &&
		    BoundBetweenExpression::HasValidBindData(function)) {
			HudiFilterSpec lower, upper;
			if (ResolveComparison(BoundBetweenExpression::Input(function), BoundBetweenExpression::LowerBound(function),
			                      BoundBetweenExpression::LowerInclusive(function) ? ">=" : ">", lower)) {
				out.push_back(std::move(lower));
			}
			if (ResolveComparison(BoundBetweenExpression::Input(function), BoundBetweenExpression::UpperBound(function),
			                      BoundBetweenExpression::UpperInclusive(function) ? "<=" : "<", upper)) {
				out.push_back(std::move(upper));
			}
			return;
		}
		return;
	}
	case ExpressionClass::BOUND_OPERATOR: {
		auto &op = expr.Cast<BoundOperatorExpression>();
		auto type = op.GetExpressionType();
		// `NOT (col = v1 OR col = v2 ...)` is how 2.0's in-clause rewriter
		// delivers `col NOT IN (v1, v2, ...)`; translate it as NOT IN.
		if (type == ExpressionType::OPERATOR_NOT) {
			// 2.0 shape: NOT(IN(col, a, b)); 1.x shape: NOT(col = a OR col = b)
			if (TryTranslateNotIn(expr, out) || TryTranslateNegatedOr(expr, out)) {
				return;
			}
			return;
		}
		if (type != ExpressionType::COMPARE_IN && type != ExpressionType::COMPARE_NOT_IN) {
			return;
		}
		auto &children = op.GetChildren();
		if (children.size() < 2) {
			return;
		}
		FilterColumn column;
		if (!ResolveColumn(*children[0], column)) {
			return;
		}
		HudiFilterSpec spec;
		spec.column = column.name;
		spec.op = type == ExpressionType::COMPARE_IN ? "IN" : "NOT IN";
		for (idx_t i = 1; i < children.size(); i++) {
			string literal;
			if (!ResolveLiteral(*children[i], column, literal)) {
				return;
			}
			spec.values.push_back(std::move(literal));
		}
		out.push_back(std::move(spec));
		return;
	}
	default:
		return;
	}
}

} // namespace

void HudiPushdownComplexFilter(ClientContext &context, LogicalGet &get, FunctionData *bind_data_p,
                               vector<unique_ptr<Expression>> &filters) {
	if (!bind_data_p) {
		return;
	}
	auto &bind = bind_data_p->Cast<HudiScanBindData>();
	FilterTranslator translator(get, bind.names, bind.types);
	vector<HudiFilterSpec> translated;
	for (auto &filter : filters) {
		translator.Translate(*filter, translated);
	}
	bind.filters = std::move(translated);
}

} // namespace duckdb
