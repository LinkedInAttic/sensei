#!/bin/sh
#
# Run this wish script to generate syntax bubble diagrams from
# text descriptions.
#
# $Id: BQL-diagrams.tcl,v 1.6 2011/11/06 07:13:21 bcui Exp $
#

#The next line executes wish - wherever it is \
exec wish "$0" "$@"

set all_graphs {
    statement {
	line 
	{or 
	    describe
	    select_stmt
	} 
	{opt ;}
    }

    select_stmt {
	stack
	{line SELECT {or * {loop column_name ,}}
	    FROM index_name
	    {optx where_clause}
	}
	{line {opt given_clause}
	    {opt
		{loop
		    {line 
			{or 
			    order_by_clause
			    group_by_clause
			    limit_clause
			    browse_by_clause
			    fetching_stored_clause
			}
		    }
		    nil
		}
	    }
	}
    }

    describe_stmt {
	line {or DESC DESCRIBE} index_name
    }

    where_clause {
	line WHERE {or predicates cumulative_predicates}
    }

    predicates {
	loop
	{or 
	    in_predicate
	    contains_all_predicate
	    equal_predicate
	    not_equal_predicate
	    query_predicate
	    between_predicate
	    range_predicate
	    time_predicate
	    same_columns_or_pred
	}
	AND
    }

    cumulative_predicates {
	loop
	{or
	    in_predicate
	    equal_predicate
	    between_predicate
	    range_predicate
	}
	OR
    }

    in_predicate {
	stack
	{line column_name {opt NOT} IN value_list}
	{line {opt except_clause} {opt predicate_props}}
    }

    contains_all_predicate {
	stack
	{line column_name CONTAINS ALL value_list}
	{line {opt except_clause} {opt predicate_props}}
    }

    equal_predicate {
	line column_name = value {opt predicate_props}
    } 

    not_equal_predicate {
	line column_name <> value {opt predicate_props}
    } 

    query_predicate {
	line QUERY IS quoted_string
    }

    between_predicate {
	line column_name {opt NOT} BETWEEN value AND value
    }

    range_predicate {
	line column_name {or < <= >= >} numeric
    }

    time_predicate {
	line column_name
	{or
	    {line IN LAST time_span}
	    {line {or SINCE AFTER BEFORE} time_expr}
	}
    }

    except_clause {
	line EXCEPT value_list
    }

    predicate_props {
	line WITH ( 
	{loop
	    {line quoted_string : quoted_string}
	    ,
	}
	)
    }

    given_clause {
	stack
	{line GIVEN FACET PARAM}
	{loop
	    {line
		( facet_name param_name
		{or
		    BOOLEAN
		    INT
		    LONG
		    STRING
		    BYTEARRAY
		    DOUBLE
		}
		param_value
		)
	    }
	    ,
	}
    }

    additional_clauses {
	loop
	{or
	    order_by_clause
	    group_by_clause
	    limit_clause
	    browse_by_clause
	    fetching_stored_clause
	}
	nil
    }

    order_by_clause {
	line ORDER BY
	{loop
	    {line
		column_name
		{opt {or ASC DESC}}}
	    ,
	}
    }

    group_by_clause {
	line GROUP BY facet_name
	{optx
	    {line TOP max_per_group}
	}
    }
	
    limit_clause {
	line LIMIT
	{opt {line offset ,}}
	count
    }

    browse_by_clause {
	line BROWSE BY
	{loop facet_spec ,}
    }

    facet_spec {
	line 
	facet_name
	{optx
	    (
	    {or TRUE FALSE}
	    min_hits
	    max_hits
	    {or HITS VALUE}
	    )
	}
    }

    fetching_stored_clause {
	line FETCHING STORED {opt {or TRUE FALSE}}
    }

}


set old_graphs {
  sql-stmt {
    line
      {opt EXPLAIN {opt QUERY PLAN}}
      {or
         alter-table-stmt
         analyze-stmt
         attach-stmt
         begin-stmt
         commit-stmt
         create-index-stmt
         create-table-stmt
         create-trigger-stmt
         create-view-stmt
         create-virtual-table-stmt
         delete-stmt
         delete-stmt-limited
         detach-stmt
         drop-index-stmt
         drop-table-stmt
         drop-trigger-stmt
         drop-view-stmt
         insert-stmt
         pragma-stmt
         reindex-stmt
         release-stmt
         rollback-stmt
         savepoint-stmt
         select-stmt
         update-stmt
         update-stmt-limited
         vacuum-stmt
      }
  }
  alter-table-stmt {
    stack
       {line ALTER TABLE {optx /database-name .} /table-name}
       {tailbranch
          {line RENAME TO /new-table-name}
          {line ADD {optx COLUMN} column-def}
       }
  }
  analyze-stmt {
     line ANALYZE {or nil /database-name /table-name
                    {line /database-name . /table-name}}
  }
  attach-stmt {
     line ATTACH {or DATABASE nil} /filename AS /database-name
  }
  begin-stmt {
     line BEGIN {or nil DEFERRED IMMEDIATE EXCLUSIVE}
          {optx TRANSACTION}
  }
  commit-stmt {
     line {or COMMIT END} {optx TRANSACTION}
  }
  rollback-stmt {
     line ROLLBACK {optx TRANSACTION}
        {optx TO {optx SAVEPOINT} /savepoint-name}
  }
  savepoint-stmt {
     line SAVEPOINT /savepoint-name
  }
  release-stmt {
     line RELEASE {optx SAVEPOINT} /savepoint-name
  }
  create-index-stmt {
    stack
       {line CREATE {opt UNIQUE} INDEX {opt IF NOT EXISTS}}
       {line {optx /database-name .} /index-name
             ON /table-name ( {loop indexed-column ,} )}
  }
  indexed-column {
      line /column-name {optx COLLATE /collation-name} {or ASC DESC nil} 
  }
  create-table-stmt {
    stack
       {line CREATE {or {} TEMP TEMPORARY} TABLE {opt IF NOT EXISTS}}
       {line {optx /database-name .} /table-name
          {tailbranch
            {line ( {loop column-def ,} {loop {} {, table-constraint}} )}
            {line AS select-stmt}
          }
       }
  }
  column-def {
    line /column-name {or type-name nil} {loop nil {nil column-constraint nil}}
  }
  type-name {
     line {loop /name {}} {or {}
        {line ( signed-number )}
        {line ( signed-number , signed-number )}
     }
  }
  column-constraint {
    stack
      {optx CONSTRAINT /name}
      {or
         {line PRIMARY KEY {or nil ASC DESC} 
               conflict-clause {opt AUTOINCREMENT}
         }
         {line NOT NULL conflict-clause}
         {line UNIQUE conflict-clause}
         {line CHECK ( expr )}
         {line DEFAULT 
            {or
                signed-number
                literal-value
                {line ( expr )}
            }
         }
         {line COLLATE /collation-name}
         {line foreign-key-clause}
      }
  }
  signed-number {
     line
        {or nil + -}
        {or /integer-literal /floating-point-literal}
  }
  table-constraint {
     stack
       {optx CONSTRAINT /name}
       {or
          {line {or {line PRIMARY KEY} UNIQUE}
                ( {loop indexed-column ,} ) conflict-clause}
          {line CHECK ( expr )}
          {line FOREIGN KEY ( {loop /column-name ,} ) foreign-key-clause }
       }
  }
  foreign-key-clause {
      stack 
        {line REFERENCES /foreign-table {optx ( {loop /column-name ,} )}}
          {loop
             {or
                {line ON {or DELETE UPDATE INSERT}
                         {or {line SET NULL} {line SET DEFAULT}
                             CASCADE RESTRICT
                         }
                }
                {line MATCH /name}
             }
             {}
          }
        {or
          {line {optx NOT} DEFERRABLE 
             {or 
                {line INITIALLY DEFERRED}
                {line INITIALLY IMMEDIATE}
                {}
             }
          }
          nil
        }
  }
  conflict-clause {
    opt {line ON CONFLICT {or ROLLBACK ABORT FAIL IGNORE REPLACE}}
  }
  create-trigger-stmt {
    stack
       {line CREATE {or {} TEMP TEMPORARY} TRIGGER {opt IF NOT EXISTS}}
       {line {optx /database-name .} /trigger-name
             {or BEFORE AFTER {line INSTEAD OF} nil}
       }
       {line
             {or DELETE INSERT 
                 {line UPDATE {opt OF {loop /column-name ,} }}
             }
             ON /table-name
       }
       {line {optx FOR EACH ROW}
             {optx WHEN expr}
       }
       {line BEGIN
             {loop 
                {line {or update-stmt insert-stmt delete-stmt select-stmt} ;} 
                nil
             }
             END
       }
  }
  create-view-stmt {
    stack
       {line CREATE {or {} TEMP TEMPORARY} VIEW {opt IF NOT EXISTS}}
       {line {optx /database-name .} /view-name AS select-stmt}
  }
  create-virtual-table-stmt {
    stack
       {line CREATE VIRTUAL TABLE {optx /database-name .} /table-name}
       {line USING /module-name {optx ( {loop module-argument ,} )}}
  }
  delete-stmt {
    line DELETE FROM qualified-table-name {optx WHERE expr}
  }
  delete-stmt-limited {
    stack
        {line DELETE FROM qualified-table-name {optx WHERE expr}}
        {optx
            {stack
              {optx ORDER BY {loop ordering-term ,}}
              {line LIMIT /integer {optx {or OFFSET ,} /integer}}
            }
        }
  }
  detach-stmt {
    line DETACH {optx DATABASE} /database-name
  }
  drop-index-stmt {
    line DROP INDEX {optx IF EXISTS} {optx /database-name .} /index-name
  }
  drop-table-stmt {
    line DROP TABLE {optx IF EXISTS} {optx /database-name .} /table-name
  }
  drop-trigger-stmt {
    line DROP TRIGGER {optx IF EXISTS} {optx /database-name .} /trigger-name
  }
  drop-view-stmt {
    line DROP VIEW {optx IF EXISTS} {optx /database-name .} /view-name
  }
  expr {
    or
     {line literal-value}
     {line bind-parameter}
     {line {optx {optx /database-name .} /table-name .} /column-name}
     {line /unary-operator expr}
     {line expr /binary-operator expr}
     {line /function-name ( {or {line {optx DISTINCT} {toploop expr ,}} {} *} )}
     {line ( expr )}
     {line CAST ( expr AS type-name )}
     {line expr COLLATE /collation-name}
     {line expr {optx NOT} {or LIKE GLOB REGEXP MATCH} expr
           {optx ESCAPE expr}}
     {line expr {or ISNULL NOTNULL {line IS NULL} {line NOT NULL}
                                   {line IS NOT NULL}}}
     {line expr {optx NOT} BETWEEN expr AND expr}
     {line expr {optx NOT} IN 
            {or
               {line ( {or {} select-stmt {loop expr ,}} )}
               {line {optx /database-name .} /table-name}
            }
     }
     {line {optx {optx NOT} EXISTS} ( select-stmt )}
     {line CASE {optx expr} {loop {line WHEN expr THEN expr} {}}
           {optx ELSE expr} END}
     {line raise-function}
  }
  raise-function {
     line RAISE ( 
           {or IGNORE
               {line {or ROLLBACK ABORT FAIL} , /error-message }
           } )
  }
  literal-value {
    or
     {line /integer-literal}
     {line /floating-point-literal}
     {line /string-literal}
     {line /blob-literal}
     {line NULL}
     {line CURRENT_TIME}
     {line CURRENT_DATE}
     {line CURRENT_TIMESTAMP}
  }
  insert-stmt {
    stack
       {line
          {or 
              {line INSERT {opt OR {or ROLLBACK ABORT REPLACE FAIL IGNORE}}}
              REPLACE
          }
          INTO {optx /database-name .} /table-name
       }
       {tailbranch
          {line 
                {optx ( {loop /column-name ,} )}
                {tailbranch
                    {line VALUES ( {loop expr ,} )}
                    select-stmt
                }
          }
          {line DEFAULT VALUES}
       }
  }
  pragma-stmt {
     line PRAGMA {optx /database-name .} /pragma-name
          {or
              nil
              {line = pragma-value}
              {line ( pragma-value )}
          }
  }
  pragma-value {
     or
        signed-number
        /name
        /string-literal
  }
  reindex-stmt {
     line REINDEX
          {tailbranch
             /collation-name
             {line {optx /database-name .}
                 {tailbranch /table-name /index-name}
             }
          }
  }
  select-stmt {
    stack
       {loop {line select-core nil} {nil compound-operator nil}}
       {optx ORDER BY {loop ordering-term ,}}
       {optx LIMIT /integer {optx {or OFFSET ,} /integer}}
  }
  select-core {
     stack
       {line SELECT {or nil DISTINCT ALL} {loop result-column ,}}
       {optx FROM join-source}
       {optx WHERE expr}
       {optx GROUP BY {loop ordering-term ,} {optx HAVING expr}}
        
  }
  result-column {
     or
        *
        {line /table-name . *}
        {line expr {optx {optx AS} /column-alias}}
  }
  join-source {
     line
        single-source
        {opt {loop {line nil join-op single-source join-constraint nil} {}}}
  }
  single-source {
     or
       {line
          {optx /database-name .} /table-name
          {optx {optx AS} /table-alias}
          {or nil {line INDEXED BY /index-name} {line NOT INDEXED}}
       }
       {line
          ( select-stmt ) {optx {optx AS} /table-alias}
       }
       {line ( join-source )}
  }
  join-op {
     or
        {line ,}
        {line
            {opt NATURAL}
            {or {line {opt LEFT} {opt OUTER}} INNER CROSS}
            JOIN
        }
  }
  join-constraint {
     or
        {line ON expr}
        {line USING ( {loop /column-name ,} )}
        nil
  }
  ordering-term {
      line expr {opt COLLATE /collation-name} {or nil ASC DESC} 
  }
  compound-operator {
     or {line UNION {optx ALL}} INTERSECT EXCEPT
  }
  update-stmt {
     stack
        {line UPDATE {opt OR {or ROLLBACK ABORT REPLACE FAIL IGNORE}}
              qualified-table-name}
        {line SET {loop {line /column-name = expr} ,} {optx WHERE expr}}
  }
  update-stmt-limited {
     stack
        {line UPDATE {opt OR {or ROLLBACK ABORT REPLACE FAIL IGNORE}}
              qualified-table-name}
        {line SET {loop {line /column-name = expr} ,} {optx WHERE expr}}
        {optx
            {stack
              {optx ORDER BY {loop ordering-term ,}}
              {line LIMIT /integer {optx {or OFFSET ,} /integer}}
            }
        }
  }
  qualified-table-name {
     line {optx /database-name .} /table-name
          {or nil {line INDEXED BY /index-name} {line NOT INDEXED}}
  }
  vacuum-stmt {
      line VACUUM
  }
  comment-syntax {
    or
      {line -- {loop nil /anything-except-newline} 
           {or /newline /end-of-input}}
      {line /* {loop nil /anything-except-*/}
           {or */ /end-of-input}}
  }
}


# Top-level displays
#
toplevel .bb
canvas .c -bg white
pack .c -side top -fill both -expand 1
wm withdraw .

# Draw the button bar
#
set bn 0
foreach {name graph} $all_graphs {
  incr bn
  set b .bb.b$bn
  button $b -text $name -command [list draw_graph $name $graph] -pady 0
  pack $b -side top -fill x -expand 1 -pady 0
}
incr bn
set b .bb.b$bn
button $b -text Everything -command {draw_all_graphs}
pack $b -side top -fill x -expand 1

set tagcnt 0                      ;# tag counter
set font1 {Helvetica 16 bold}     ;# default token font
set font2 {Helvetica 15}          ;# default variable font
set RADIUS 9                      ;# default turn radius
set HSEP 17                       ;# horizontal separation
set VSEP 9                        ;# vertical separation
set DPI 100                       ;# dots per inch


# Draw a right-hand turn around.  Approximately a ")"
#
proc draw_right_turnback {tag x y0 y1} {
  global RADIUS
  if {$y0 + 2*$RADIUS < $y1} {
    set xr0 [expr {$x-$RADIUS}]
    set xr1 [expr {$x+$RADIUS}]
    .c create arc $xr0 $y0 $xr1 [expr {$y0+2*$RADIUS}] \
          -width 2 -start 90 -extent -90 -tags $tag -style arc
    set yr0 [expr {$y0+$RADIUS}]
    set yr1 [expr {$y1-$RADIUS}]
    if {abs($yr1-$yr0)>$RADIUS*2} {
      set half_y [expr {($yr1+$yr0)/2}]
      .c create line $xr1 $yr0 $xr1 $half_y -width 2 -tags $tag -arrow last
      .c create line $xr1 $half_y $xr1 $yr1 -width 2 -tags $tag
    } else {
      .c create line $xr1 $yr0 $xr1 $yr1 -width 2 -tags $tag
    }
    .c create arc $xr0 [expr {$y1-2*$RADIUS}] $xr1 $y1 \
          -width 2 -start 0 -extent -90 -tags $tag -style arc
  } else { 
    set r [expr {($y1-$y0)/2.0}]
    set x0 [expr {$x-$r}]
    set x1 [expr {$x+$r}]
    .c create arc $x0 $y0 $x1 $y1 \
          -width 2 -start 90 -extent -180 -tags $tag -style arc
  }
}

# Draw a left-hand turn around.  Approximatley a "("
#
proc draw_left_turnback {tag x y0 y1 dir} {
  global RADIUS
  if {$y0 + 2*$RADIUS < $y1} {
    set xr0 [expr {$x-$RADIUS}]
    set xr1 [expr {$x+$RADIUS}]
    .c create arc $xr0 $y0 $xr1 [expr {$y0+2*$RADIUS}] \
          -width 2 -start 90 -extent 90 -tags $tag -style arc
    set yr0 [expr {$y0+$RADIUS}]
    set yr1 [expr {$y1-$RADIUS}]
    if {abs($yr1-$yr0)>$RADIUS*3} {
      set half_y [expr {($yr1+$yr0)/2}]
      if {$dir=="down"} {
        .c create line $xr0 $yr0 $xr0 $half_y -width 2 -tags $tag -arrow last
        .c create line $xr0 $half_y $xr0 $yr1 -width 2 -tags $tag
      } else {
        .c create line $xr0 $yr1 $xr0 $half_y -width 2 -tags $tag -arrow last
        .c create line $xr0 $half_y $xr0 $yr0 -width 2 -tags $tag
      }
    } else {
      .c create line $xr0 $yr0 $xr0 $yr1 -width 2 -tags $tag
    }
    # .c create line $xr0 $yr0 $xr0 $yr1 -width 2 -tags $tag
    .c create arc $xr0 [expr {$y1-2*$RADIUS}] $xr1 $y1 \
          -width 2 -start 180 -extent 90 -tags $tag -style arc
  } else { 
    set r [expr {($y1-$y0)/2.0}]
    set x0 [expr {$x-$r}]
    set x1 [expr {$x+$r}]
    .c create arc $x0 $y0 $x1 $y1 \
          -width 2 -start 90 -extent 180 -tags $tag -style arc
  }
}

# Draw a bubble containing $txt. 
#
proc draw_bubble {txt} {
  global tagcnt
  incr tagcnt
  set tag x$tagcnt
  if {$txt=="nil"} {
    .c create line 0 0 1 0 -width 2 -tags $tag
    return [list $tag 1 0]
  } elseif {$txt=="bullet"} {
    .c create oval 0 -3 6 3 -width 2 -tags $tag
    return [list $tag 6 0]
  }
  if {[regexp {^/[a-z]} $txt]} {
    set txt [string range $txt 1 end]
    set font $::font2
    set istoken 1
  } elseif {[regexp {^[a-z]} $txt]} {
    set font $::font2
    set istoken 0
  } else {
    set font $::font1
    set istoken 1
  }
  set id1 [.c create text 0 0 -anchor c -text $txt -font $font -tags $tag]
  foreach {x0 y0 x1 y1} [.c bbox $id1] break
  set h [expr {$y1-$y0+2}]
  set rad [expr {($h+1)/2}]
  set top [expr {$y0-2}]
  set btm [expr {$y1}]
  set left [expr {$x0+3*$istoken}]
  set right [expr {$x1-3*$istoken}]
  if {$left>$right} {
    set left [expr {($x0+$x1)/2}]
    set right $left
  }
  if {$istoken} {
    .c create arc [expr {$left-$rad}] $top [expr {$left+$rad}] $btm \
         -width 2 -start 90 -extent 180 -style arc -tags $tag
    .c create arc [expr {$right-$rad}] $top [expr {$right+$rad}] $btm \
         -width 2 -start -90 -extent 180 -style arc -tags $tag
    if {$left<$right} {
      .c create line $left $top $right $top -width 2 -tags $tag
      .c create line $left $btm $right $btm -width 2 -tags $tag
    }
  } else {
    .c create rect $left $top $right $btm -width 2 -tags $tag
  }
  foreach {x0 y0 x1 y1} [.c bbox $tag] break
  set width [expr {$x1-$x0}]
  .c move $tag [expr {-$x0}] 0

  # Entry is always 0 0
  # Return:  TAG EXIT_X EXIT_Y
  #
  return [list $tag $width 0]
}

# Draw a sequence of terms from left to write.  Each element of $lx
# descripts a single term.
#
proc draw_line {lx} {
  global tagcnt
  incr tagcnt
  set tag x$tagcnt

  set sep $::HSEP
  set exx 0
  set exy 0
  foreach term $lx {
    set m [draw_diagram $term]
    foreach {t texx texy} $m break
    if {$exx>0} {
      set xn [expr {$exx+$sep}]
      .c move $t $xn $exy
      .c create line [expr {$exx-1}] $exy $xn $exy \
         -tags $tag -width 2 -arrow last
      set exx [expr {$xn+$texx}]
    } else {
      set exx $texx
    }
    set exy $texy
    .c addtag $tag withtag $t
    .c dtag $t $t
  }
  if {$exx==0} {	
    set exx [expr {$sep*2}]
    .c create line 0 0 $sep 0 -width 2 -tags $tag -arrow last
    .c create line $sep 0 $exx 0 -width 2 -tags $tag
    set exx $sep
  }
  return [list $tag $exx $exy]
}

# Draw a sequence of terms from right to left.
#
proc draw_backwards_line {lx} {
  global tagcnt
  incr tagcnt
  set tag x$tagcnt

  set sep $::HSEP
  set exx 0
  set exy 0
  set lb {}
  set n [llength $lx]
  for {set i [expr {$n-1}]} {$i>=0} {incr i -1} {
    lappend lb [lindex $lx $i]
  }
  foreach term $lb {
    set m [draw_diagram $term]
    foreach {t texx texy} $m break
    foreach {tx0 ty0 tx1 ty1} [.c bbox $t] break
    set w [expr {$tx1-$tx0}]
    if {$exx>0} {
      set xn [expr {$exx+$sep}]
      .c move $t $xn 0
      .c create line $exx $exy $xn $exy -tags $tag -width 2 -arrow first
      set exx [expr {$xn+$texx}]
    } else {
      set exx $texx
    }
    set exy $texy
    .c addtag $tag withtag $t
    .c dtag $t $t
  }
  if {$exx==0} {
    .c create line 0 0 $sep 0 -width 2 -tags $tag
    set exx $sep
  }
  return [list $tag $exx $exy]
}

# Draw a sequence of terms from top to bottom.
#
proc draw_stack {indent lx} {
  global tagcnt RADIUS VSEP
  incr tagcnt
  set tag x$tagcnt

  set sep [expr {$VSEP*2}]
  set btm 0
  set n [llength $lx]
  set i 0
  set next_bypass_y 0

  foreach term $lx {
    set bypass_y $next_bypass_y
    if {$i>0 && $i<$n && [llength $term]>1 &&
        ([lindex $term 0]=="opt" || [lindex $term 0]=="optx")} {
      set bypass 1
      set term "line [lrange $term 1 end]"
    } else {
      set bypass 0
      set next_bypass_y 0
    }
    set m [draw_diagram $term]
    foreach {t exx exy} $m break
    foreach {tx0 ty0 tx1 ty1} [.c bbox $t] break
    if {$i==0} {
      set btm $ty1
      set exit_y $exy
      set exit_x $exx
    } else {
      set enter_y [expr {$btm - $ty0 + $sep*2 + 2}]
      if {$bypass} {set next_bypass_y [expr {$enter_y - $RADIUS}]}
      set enter_x [expr {$sep*2 + $indent}]
      set back_y [expr {$btm + $sep + 1}]
      if {$bypass_y>0} {
         set mid_y [expr {($bypass_y+$RADIUS+$back_y)/2}]
         .c create line $bypass_x $bypass_y $bypass_x $mid_y \
            -width 2 -tags $tag -arrow last
         .c create line $bypass_x $mid_y $bypass_x [expr {$back_y+$RADIUS}] \
             -tags $tag -width 2
      }
      .c move $t $enter_x $enter_y
      set e2 [expr {$exit_x + $sep}]
      .c create line $exit_x $exit_y $e2 $exit_y \
            -width 2 -tags $tag
      draw_right_turnback $tag $e2 $exit_y $back_y
      set e3 [expr {$enter_x-$sep}]
      set bypass_x [expr {$e3-$RADIUS}]
      set emid [expr {($e2+$e3)/2}]
      .c create line $e2 $back_y $emid $back_y \
                 -width 2 -tags $tag -arrow last
      .c create line $emid $back_y $e3 $back_y \
                 -width 2 -tags $tag
      set r2 [expr {($enter_y - $back_y)/2.0}]
      draw_left_turnback $tag $e3 $back_y $enter_y down
      .c create line $e3 $enter_y $enter_x $enter_y \
                 -arrow last -width 2 -tags $tag
      set exit_x [expr {$enter_x + $exx}]
      set exit_y [expr {$enter_y + $exy}]
    }
    .c addtag $tag withtag $t
    .c dtag $t $t
    set btm [lindex [.c bbox $tag] 3]
    incr i
  }
  if {$bypass} {
    set fwd_y [expr {$btm + $sep + 1}]
    set mid_y [expr {($next_bypass_y+$RADIUS+$fwd_y)/2}]
    set descender_x [expr {$exit_x+$RADIUS}]
    .c create line $bypass_x $next_bypass_y $bypass_x $mid_y \
        -width 2 -tags $tag -arrow last
    .c create line $bypass_x $mid_y $bypass_x [expr {$fwd_y-$RADIUS}] \
        -tags $tag -width 2
    .c create arc $bypass_x [expr {$fwd_y-2*$RADIUS}] \
                  [expr {$bypass_x+2*$RADIUS}] $fwd_y \
        -width 2 -start 180 -extent 90 -tags $tag -style arc
    .c create arc [expr {$exit_x-$RADIUS}] $exit_y \
                  $descender_x [expr {$exit_y+2*$RADIUS}] \
        -width 2 -start 90 -extent -90 -tags $tag -style arc
    .c create arc $descender_x [expr {$fwd_y-2*$RADIUS}] \
                  [expr {$descender_x+2*$RADIUS}] $fwd_y \
        -width 2 -start 180 -extent 90 -tags $tag -style arc
    set exit_x [expr {$exit_x+2*$RADIUS}]
    set half_x [expr {($exit_x+$indent)/2}]
    .c create line [expr {$bypass_x+$RADIUS}] $fwd_y $half_x $fwd_y \
        -width 2 -tags $tag -arrow last
    .c create line $half_x $fwd_y $exit_x $fwd_y \
        -width 2 -tags $tag
    .c create line $descender_x [expr {$exit_y+$RADIUS}] \
                   $descender_x [expr {$fwd_y-$RADIUS}] \
        -width 2 -tags $tag -arrow last
    set exit_y $fwd_y
  }
  set width [lindex [.c bbox $tag] 2]
  return [list $tag $exit_x $exit_y]
}

proc draw_loop {forward back} {
  global tagcnt
  incr tagcnt
  set tag x$tagcnt
  set sep $::HSEP
  set vsep $::VSEP
  if {$back==","} {
    set vsep 0
  } elseif {$back=="nil"} {
    set vsep [expr {$vsep/2}]
  }

  foreach {ft fexx fexy} [draw_diagram $forward] break
  foreach {fx0 fy0 fx1 fy1} [.c bbox $ft] break
  set fw [expr {$fx1-$fx0}]
  foreach {bt bexx bexy} [draw_backwards_line $back] break
  foreach {bx0 by0 bx1 by1} [.c bbox $bt] break
  set bw [expr {$bx1-$bx0}]
  set dy [expr {$fy1 - $by0 + $vsep}]
  .c move $bt 0 $dy
  set biny $dy
  set bexy [expr {$dy+$bexy}]
  set by0 [expr {$dy+$by0}]
  set by1 [expr {$dy+$by1}]

  if {$fw>$bw} {
    if {$fexx<$fw && $fexx>=$bw} {
      set dx [expr {($fexx-$bw)/2}]
      .c move $bt $dx 0
      set bexx [expr {$dx+$bexx}]
      .c create line 0 $biny $dx $biny -width 2 -tags $bt
      .c create line $bexx $bexy $fexx $bexy -width 2 -tags $bt -arrow first
      set mxx $fexx
    } else {
      set dx [expr {($fw-$bw)/2}]
      .c move $bt $dx 0
      set bexx [expr {$dx+$bexx}]
      .c create line 0 $biny $dx $biny -width 2 -tags $bt
      .c create line $bexx $bexy $fx1 $bexy -width 2 -tags $bt -arrow first
      set mxx $fexx
    }
  } elseif {$bw>$fw} {
    set dx [expr {($bw-$fw)/2}]
    .c move $ft $dx 0
    set fexx [expr {$dx+$fexx}]
    .c create line 0 0 $dx $fexy -width 2 -tags $ft -arrow last
    .c create line $fexx $fexy $bx1 $fexy -width 2 -tags $ft
    set mxx $bexx
  }
  .c addtag $tag withtag $bt
  .c addtag $tag withtag $ft
  .c dtag $bt $bt
  .c dtag $ft $ft
  .c move $tag $sep 0
  set mxx [expr {$mxx+$sep}]
  .c create line 0 0 $sep 0 -width 2 -tags $tag
  draw_left_turnback $tag $sep 0 $biny up
  draw_right_turnback $tag $mxx $fexy $bexy
  foreach {x0 y0 x1 y1} [.c bbox $tag] break
  set exit_x [expr {$mxx+$::RADIUS}]
  .c create line $mxx $fexy $exit_x $fexy -width 2 -tags $tag
  return [list $tag $exit_x $fexy]
}

proc draw_toploop {forward back} {
  global tagcnt
  incr tagcnt
  set tag x$tagcnt
  set sep $::VSEP
  set vsep [expr {$sep/2}]

  foreach {ft fexx fexy} [draw_diagram $forward] break
  foreach {fx0 fy0 fx1 fy1} [.c bbox $ft] break
  set fw [expr {$fx1-$fx0}]
  foreach {bt bexx bexy} [draw_backwards_line $back] break
  foreach {bx0 by0 bx1 by1} [.c bbox $bt] break
  set bw [expr {$bx1-$bx0}]
  set dy [expr {-($by1 - $fy0 + $vsep)}]
  .c move $bt 0 $dy
  set biny $dy
  set bexy [expr {$dy+$bexy}]
  set by0 [expr {$dy+$by0}]
  set by1 [expr {$dy+$by1}]

  if {$fw>$bw} {
    set dx [expr {($fw-$bw)/2}]
    .c move $bt $dx 0
    set bexx [expr {$dx+$bexx}]
    .c create line 0 $biny $dx $biny -width 2 -tags $bt
    .c create line $bexx $bexy $fx1 $bexy -width 2 -tags $bt -arrow first
    set mxx $fexx
  } elseif {$bw>$fw} {
    set dx [expr {($bw-$fw)/2}]
    .c move $ft $dx 0
    set fexx [expr {$dx+$fexx}]
    .c create line 0 0 $dx $fexy -width 2 -tags $ft
    .c create line $fexx $fexy $bx1 $fexy -width 2 -tags $ft
    set mxx $bexx
  }
  .c addtag $tag withtag $bt
  .c addtag $tag withtag $ft
  .c dtag $bt $bt
  .c dtag $ft $ft
  .c move $tag $sep 0
  set mxx [expr {$mxx+$sep}]
  .c create line 0 0 $sep 0 -width 2 -tags $tag
  draw_left_turnback $tag $sep 0 $biny down
  draw_right_turnback $tag $mxx $fexy $bexy
  foreach {x0 y0 x1 y1} [.c bbox $tag] break
  .c create line $mxx $fexy $x1 $fexy -width 2 -tags $tag
  return [list $tag $x1 $fexy]
}

proc draw_or {lx} {
  global tagcnt
  incr tagcnt
  set tag x$tagcnt
  set sep $::VSEP
  set vsep [expr {$sep/2}]
  set n [llength $lx]
  set i 0
  set mxw 0
  foreach term $lx {
    set m($i) [set mx [draw_diagram $term]]
    set tx [lindex $mx 0]
    foreach {x0 y0 x1 y1} [.c bbox $tx] break
    set w [expr {$x1-$x0}]
    if {$i>0} {set w [expr {$w+20}]}  ;# extra space for arrowheads
    if {$w>$mxw} {set mxw $w}
    incr i
  }

  set x0 0                        ;# entry x
  set x1 $sep                     ;# decender 
  set x2 [expr {$sep*2}]          ;# start of choice
  set xc [expr {$mxw/2}]          ;# center point
  set x3 [expr {$mxw+$x2}]        ;# end of choice
  set x4 [expr {$x3+$sep}]        ;# accender
  set x5 [expr {$x4+$sep}]        ;# exit x

  for {set i 0} {$i<$n} {incr i} {
    foreach {t texx texy} $m($i) break
    foreach {tx0 ty0 tx1 ty1} [.c bbox $t] break
    set w [expr {$tx1-$tx0}]
    set dx [expr {($mxw-$w)/2 + $x2}]
    if {$w>10 && $dx>$x2+10} {set dx [expr {$x2+10}]}
    .c move $t $dx 0
    set texx [expr {$texx+$dx}]
    set m($i) [list $t $texx $texy]
    foreach {tx0 ty0 tx1 ty1} [.c bbox $t] break
    if {$i==0} {
      if {$dx>$x2} {set ax last} {set ax none}
      .c create line 0 0 $dx 0 -width 2 -tags $tag -arrow $ax
      .c create line $texx $texy [expr {$x5+1}] $texy -width 2 -tags $tag
      set exy $texy
      .c create arc -$sep 0 $sep [expr {$sep*2}] \
         -width 2 -start 90 -extent -90 -tags $tag -style arc
      set btm $ty1
    } else {
      set dy [expr {$btm - $ty0 + $vsep}]
      if {$dy<2*$sep} {set dy [expr {2*$sep}]}
      .c move $t 0 $dy
      set texy [expr {$texy+$dy}]
      if {$dx>$x2} {
        .c create line $x2 $dy $dx $dy -width 2 -tags $tag -arrow last
        if {$dx<$xc-2} {set ax last} {set ax none}
        .c create line $texx $texy $x3 $texy -width 2 -tags $tag -arrow $ax
      }
      set y1 [expr {$dy-2*$sep}]
      .c create arc $x1 $y1 [expr {$x1+2*$sep}] $dy \
          -width 2 -start 180 -extent 90 -style arc -tags $tag
      set y2 [expr {$texy-2*$sep}]
      .c create arc [expr {$x3-$sep}] $y2 $x4 $texy \
          -width 2 -start 270 -extent 90 -style arc -tags $tag
      if {$i==$n-1} {
        .c create arc $x4 $exy [expr {$x4+2*$sep}] [expr {$exy+2*$sep}] \
           -width 2 -start 180 -extent -90 -tags $tag -style arc
        .c create line $x1 [expr {$dy-$sep}] $x1 $sep -width 2 -tags $tag
        .c create line $x4 [expr {$texy-$sep}] $x4 [expr {$exy+$sep}] \
               -width 2 -tags $tag
      }
      set btm [expr {$ty1+$dy}]
    }
    .c addtag $tag withtag $t
    .c dtag $t $t
  }
  return [list $tag $x5 $exy]   
}

proc draw_tail_branch {lx} {
  global tagcnt
  incr tagcnt
  set tag x$tagcnt
  set sep $::VSEP
  set vsep [expr {$sep/2}]
  set n [llength $lx]
  set i 0
  foreach term $lx {
    set m($i) [set mx [draw_diagram $term]]
    incr i
  }

  set x0 0                        ;# entry x
  set x1 $sep                     ;# decender 
  set x2 [expr {$sep*2}]          ;# start of choice

  for {set i 0} {$i<$n} {incr i} {
    foreach {t texx texy} $m($i) break
    foreach {tx0 ty0 tx1 ty1} [.c bbox $t] break
    set dx [expr {$x2+10}]
    .c move $t $dx 0
    foreach {tx0 ty0 tx1 ty1} [.c bbox $t] break
    if {$i==0} {
      .c create line 0 0 $dx 0 -width 2 -tags $tag -arrow last
      .c create arc -$sep 0 $sep [expr {$sep*2}] \
         -width 2 -start 90 -extent -90 -tags $tag -style arc
      set btm $ty1
    } else {
      set dy [expr {$btm - $ty0 + $vsep}]
      if {$dy<2*$sep} {set dy [expr {2*$sep}]}
      .c move $t 0 $dy
      if {$dx>$x2} {
        .c create line $x2 $dy $dx $dy -width 2 -tags $tag -arrow last
      }
      set y1 [expr {$dy-2*$sep}]
      .c create arc $x1 $y1 [expr {$x1+2*$sep}] $dy \
          -width 2 -start 180 -extent 90 -style arc -tags $tag
      if {$i==$n-1} {
        .c create line $x1 [expr {$dy-$sep}] $x1 $sep -width 2 -tags $tag
      }
      set btm [expr {$ty1+$dy}]
    }
    .c addtag $tag withtag $t
    .c dtag $t $t
  }
  return [list $tag 0 0]
}

proc draw_diagram {spec} {
  set n [llength $spec]
  if {$n==1} {
    return [draw_bubble $spec]
  }
  if {$n==0} {
    return [draw_bubble nil]
  }
  set cmd [lindex $spec 0]
  if {$cmd=="line"} {
    return [draw_line [lrange $spec 1 end]]
  }
  if {$cmd=="stack"} {
    return [draw_stack 0 [lrange $spec 1 end]]
  }
  if {$cmd=="indentstack"} {
    return [draw_stack $::HSEP [lrange $spec 1 end]]
  }
  if {$cmd=="loop"} {
    return [draw_loop [lindex $spec 1] [lindex $spec 2]]
  }
  if {$cmd=="toploop"} {
    return [draw_toploop [lindex $spec 1] [lindex $spec 2]]
  }
  if {$cmd=="or"} {
    return [draw_or [lrange $spec 1 end]]
  }
  if {$cmd=="opt"} {
    set args [lrange $spec 1 end]
    if {[llength $args]==1} {
      return [draw_or [list nil [lindex $args 0]]]
    } else {
      return [draw_or [list nil "line $args"]]
    }
  }
  if {$cmd=="optx"} {
    set args [lrange $spec 1 end]
    if {[llength $args]==1} {
      return [draw_or [list [lindex $args 0] nil]]
    } else {
      return [draw_or [list "line $args" nil]]
    }
  }
  if {$cmd=="tailbranch"} {
    # return [draw_tail_branch [lrange $spec 1 end]]
    return [draw_or [lrange $spec 1 end]]
  }
  error "unknown operator: $cmd"
}

proc draw_graph {name spec {do_xv 1}} {
  .c delete all
  wm deiconify .
  wm title . $name
  draw_diagram "line bullet [list $spec] bullet"
  foreach {x0 y0 x1 y1} [.c bbox all] break
  .c move all [expr {2-$x0}] [expr {2-$y0}]
  foreach {x0 y0 x1 y1} [.c bbox all] break
  .c config -width $x1 -height $y1
  update
  .c postscript -file $name.ps -width [expr {$x1+2}] -height [expr {$y1+2}]
  global DPI
  exec convert -density ${DPI}x$DPI -antialias $name.ps $name.png
  exec ps2pdf -dEPSCrop $name.ps
#   if {$do_xv} {
#     exec xv $name.gif &
#   }
}

proc draw_all_graphs {} {
  global all_graphs
  set f [open all.html w]
  foreach {name graph} $all_graphs {
    if {[regexp {^X-} $name]} continue
    puts $f "<h3>$name:</h3>"
    puts $f "<img src=\"$name.png"
    draw_graph $name $graph 0
    set img($name) 1
    set children($name) {}
    set parents($name) {}
  }
  close $f
  set order {}
  foreach {name graph} $all_graphs {
    lappend order $name
    unset -nocomplain v
    walk_graph_extract_names $graph v
    unset -nocomplain v($name)
    foreach x [array names v] {
      if {![info exists img($x)]} continue
      lappend children($name) $x
      lappend parents($x) $name
    }
  }
  set f [open syntax_linkage.tcl w]
  foreach name [lsort [array names img]] {
    set cx [lsort $children($name)]
    set px [lsort $parents($name)]
    puts $f [list set syntax_linkage($name) [list $cx $px]]
  }
  puts $f [list set syntax_order $order]
  close $f
#  wm withdraw .
}

proc walk_graph_extract_names {graph varname} {
  upvar 1 $varname v
  foreach x $graph {
    set n [llength $x]
    if {$n>1} {
      walk_graph_extract_names $x v
    } elseif {[regexp {^[a-z]} $x]} {
      set v($x) 1
    }
  }
}

draw_all_graphs
wm withdraw .
exit
