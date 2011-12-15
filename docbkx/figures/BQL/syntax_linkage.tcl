set syntax_linkage(additional_clauses) {{browse_by_clause fetching_stored_clause group_by_clause limit_clause order_by_clause} {}}
set syntax_linkage(between_predicate) {{} {cumulative_predicates predicates}}
set syntax_linkage(browse_by_clause) {facet_spec {additional_clauses select_stmt}}
set syntax_linkage(contains_all_predicate) {{except_clause predicate_props} predicates}
set syntax_linkage(cumulative_predicates) {{between_predicate equal_predicate in_predicate range_predicate} where_clause}
set syntax_linkage(describe_stmt) {{} {}}
set syntax_linkage(equal_predicate) {predicate_props {cumulative_predicates predicates}}
set syntax_linkage(except_clause) {{} {contains_all_predicate in_predicate}}
set syntax_linkage(facet_spec) {{} browse_by_clause}
set syntax_linkage(fetching_stored_clause) {{} {additional_clauses select_stmt}}
set syntax_linkage(given_clause) {{} select_stmt}
set syntax_linkage(group_by_clause) {{} {additional_clauses select_stmt}}
set syntax_linkage(in_predicate) {{except_clause predicate_props} {cumulative_predicates predicates}}
set syntax_linkage(limit_clause) {{} {additional_clauses select_stmt}}
set syntax_linkage(not_equal_predicate) {predicate_props predicates}
set syntax_linkage(order_by_clause) {{} {additional_clauses select_stmt}}
set syntax_linkage(predicate_props) {{} {contains_all_predicate equal_predicate in_predicate not_equal_predicate}}
set syntax_linkage(predicates) {{between_predicate contains_all_predicate equal_predicate in_predicate not_equal_predicate query_predicate range_predicate time_predicate} where_clause}
set syntax_linkage(query_predicate) {{} predicates}
set syntax_linkage(range_predicate) {{} {cumulative_predicates predicates}}
set syntax_linkage(select_stmt) {{browse_by_clause fetching_stored_clause given_clause group_by_clause limit_clause order_by_clause where_clause} statement}
set syntax_linkage(statement) {select_stmt {}}
set syntax_linkage(time_predicate) {{} predicates}
set syntax_linkage(where_clause) {{cumulative_predicates predicates} select_stmt}
set syntax_order {statement select_stmt describe_stmt where_clause predicates cumulative_predicates in_predicate contains_all_predicate equal_predicate not_equal_predicate query_predicate between_predicate range_predicate time_predicate except_clause predicate_props given_clause additional_clauses order_by_clause group_by_clause limit_clause browse_by_clause facet_spec fetching_stored_clause}
