// adjlib.q — corporate action adjustment library.
//
// Adjusts OHLCV prices and volumes using daily cumulative factors from
// ref_adj_factors (populated by ref_tables.q / ref_ingest.py).
//
// Adjustment convention (matching ref_ingest.py):
//   A 2:1 split has cumulative_factor = 0.5 on pre-split dates.
//   adjusted_price  = raw_price  * cumulative_factor
//   adjusted_volume = raw_volume / cumulative_factor  (split doubles share count)
//
// This gives a continuous series where all prices are expressed in
// post-split (current) terms.
//
// Point-in-time (PIT) semantics:
//   ref_adj_factors includes a loaded_at timestamp recording when each batch
//   of factors was ingested.  Multiple revisions of the same (sym,date) factor
//   may exist with different loaded_at values.  getAdjustedClose accepts an
//   optional asOf timestamp: only factor rows with loaded_at <= asOf are used,
//   and the most recent revision within that window is selected.  Pass 0Np
//   (null) to use all available factors (i.e. the current/latest revision).

\l schema/schema.q
\l code/reference/ref_tables.q

if[not `lg in key `.;
    .lg.o:{[proc;msg] -1 (string .z.p)," [",string[proc],"] ",msg;}
 ];

// ---------------------------------------------------------------------------
// applyAdj — apply cumulative price/volume adjustments to an ohlcv_1m table.
//
// Parameters:
//   ohlcvData - table with columns matching ohlcv_1m (date sym time open high
//               low close volume instrument_id)
//   factors   - table with at least columns: sym, date, cumulative_factor
//               (typically a subset of ref_adj_factors)
//
// Returns ohlcvData with:
//   open/high/low/close multiplied by cumulative_factor
//   volume divided by cumulative_factor  (integer result)
//   all other columns unchanged
//   same column order as ohlcv_1m
//
// Logs a one-line summary: bar count, sym count, factor range.
// ---------------------------------------------------------------------------
applyAdj:{[ohlcvData;factors]
    // Build a keyed lookup table: (sym,date) -> cumulative_factor only
    kf:`sym`date xkey select sym,date,cumulative_factor from factors;

    // Left join: each bar gets its factor for that exact (sym,date).
    // Bars whose date is not in the factor table get a null factor.
    joined:ohlcvData lj kf;

    // Fill missing factors with 1.0 so unadjusted bars pass through unchanged
    joined:update cumulative_factor:1.0^cumulative_factor from joined;

    // Vectorised price adjustment (multiply) and volume adjustment (divide)
    adjusted:update
        open:  open  * cumulative_factor,
        high:  high  * cumulative_factor,
        low:   low   * cumulative_factor,
        close: close * cumulative_factor,
        volume:`long$volume % cumulative_factor
    from joined;

    // Summary log
    if[count adjusted;
        .lg.o[`adjlib;
            "applyAdj: ",string[count adjusted]," bar(s), ",
            string[count distinct adjusted`sym]," sym(s), factor range [",
            string[min joined`cumulative_factor],", ",
            string[max joined`cumulative_factor],"]"]
    ];

    // Return only the ohlcv_1m columns — drop the cumulative_factor helper
    (cols ohlcv_1m) # adjusted
 };

// ---------------------------------------------------------------------------
// getUnadjusted — return raw (unadjusted) bars from the HDB.
//
// Parameters:
//   tbl       - `trades or `ohlcv_1m
//   syms      - symbol or list of symbols, e.g. `AAPL or `AAPL`MSFT
//   startDate - first date (inclusive)
//   endDate   - last date (inclusive)
// ---------------------------------------------------------------------------
getUnadjusted:{[tbl;syms;startDate;endDate]
    syms:$[-11h=type syms; enlist syms; syms];   // normalise atom to list
    $[tbl=`trades;
        select from trades   where date within (startDate;endDate), sym in syms;
      tbl=`ohlcv_1m;
        select from ohlcv_1m where date within (startDate;endDate), sym in syms;
      '"unsupported table: ",string tbl
    ]
 };

// ---------------------------------------------------------------------------
// getAdjustedClose — OHLCV bars with an adj_close column appended.
//
// Parameters:
//   syms      - symbol or list of symbols
//   startDate - date range start (inclusive)
//   endDate   - date range end (inclusive)
//   method    - `backward  prices in post-split (current) terms
//                           adjusted = raw * cumulative_factor
//               `forward   prices in pre-split (historical) terms
//                           adjusted = raw * cumulative_factor / first_factor
//   asOf      - timestamp (or 0Np for null): when not null, only factor rows
//               with loaded_at <= asOf are considered, and the most recent
//               revision within that window is used per (sym,date).
//               Pass 0Np to use the latest available revision (no PIT filter).
//
// Returns the ohlcv_1m columns plus adj_close (the adjusted close price).
// When ref_adj_factors has no entry for a (sym,date), adj_close equals close.
//
// Logs:
//   1. Header: syms, date range, method, asOf
//   2. Per-event detail: each corp action event that affects bars in range
//   3. Coverage gap warning: bar dates with no factor entry (silent 1.0 default)
//   4. Boundary continuity: adj_close either side of each split in range
// ---------------------------------------------------------------------------
getAdjustedClose:{[syms;startDate;endDate;method;asOf]
    syms:$[-11h=type syms; enlist syms; syms];
    if[not method in `backward`forward;
        '"unsupported method: ",string method
    ];

    // 1. Header
    .lg.o[`adjlib;
        "getAdjustedClose: ",(", " sv string syms),
        " ",string[startDate],"..",string[endDate],
        " method=",string[method],
        $[null asOf;" asOf=latest";" asOf=",string asOf]];

    // Raw bars from HDB (or in-memory ohlcv_1m if HDB not loaded in tests)
    bars:select from ohlcv_1m where date within (startDate;endDate), sym in syms;

    // Fetch all factor rows in range, including loaded_at for PIT filtering.
    // Guard: if ref_adj_factors lacks loaded_at (e.g. old-format data), assign
    // max timestamp (0Wp) so rows always pass the asOf filter.
    factorsRaw:$[`loaded_at in cols ref_adj_factors;
        select sym,date,cumulative_factor,loaded_at from ref_adj_factors
            where sym in syms, date within (startDate;endDate);
        update loaded_at:0Wp from select sym,date,cumulative_factor
            from ref_adj_factors where sym in syms, date within (startDate;endDate)
     ];

    // PIT filter: when asOf is given, exclude factors loaded after that time
    factorsRaw:$[null asOf; factorsRaw; select from factorsRaw where loaded_at<=asOf];

    // Deduplicate: for each (sym,date), keep the most recent revision.
    // Sort ascending by loaded_at so `last` selects the latest per group.
    factors:0!select last cumulative_factor by sym,date
        from factorsRaw iasc factorsRaw`loaded_at;
    factors:select sym,date,cumulative_factor from factors;

    if[0=count factors;
        .lg.o[`adjlib;"no adjustment factors found for ",
              (", " sv string syms)," — returning unadjusted prices"]
    ];

    // 2. Per-event detail
    if[count ref_corp_actions;
        evts:0!select from ref_corp_actions where sym in syms, ex_date > startDate;
        if[count evts;
            .lg.o[`adjlib;"events affecting this range:"];
            {[e;bars]
                nAffected:count select from bars where sym=e`sym, date < e`ex_date;
                .lg.o[`adjlib;
                    "  ",string[e`sym]," ",string[e`action_type],
                    " ex_date=",string[e`ex_date],
                    " factor=",string[e`factor],
                    " \342\206\222 ",string[nAffected]," bar(s) in range affected"]
            }[;bars] each evts
        ]
    ];

    // 3. Coverage gap warning
    if[(count bars) and count factors;
        barPairs:distinct select sym,date from bars;
        factorPairs:distinct select sym,date from factors;
        gaps:select from barPairs where not ([]sym;date) in factorPairs;
        if[count gaps;
            .lg.o[`adjlib;
                "WARNING: ",string[count gaps],
                " (sym,date) pair(s) have no factor entry — defaulting to 1.0; "
                "run: ref_ingest.py --symbols ",
                (", " sv string distinct gaps`sym)," to fix"]
        ]
    ];

    // Apply backward (post-split) price and volume adjustments — common to both methods
    adjBars:applyAdj[bars;factors];

    // Compute final result
    result:$[method=`backward;
        update adj_close:close from adjBars;
        [firstF:select first_factor:first cumulative_factor by sym
                 // Use iasc instead of xasc to avoid .Q.xasc interception in HDB sessions
                 from factors iasc factors`date;
         adjFwd:adjBars lj firstF;
         adjFwd:update first_factor:1.0^first_factor from adjFwd;
         adjFwd:update
             open:   open  % first_factor,
             high:   high  % first_factor,
             low:    low   % first_factor,
             close:  close % first_factor,
             volume: `long$(volume * first_factor)
         from adjFwd;
         (cols[ohlcv_1m],`adj_close) # update adj_close:close from adjFwd]
     ];

    // 4. Boundary continuity for splits
    if[count ref_corp_actions;
        splitEvts:0!select from ref_corp_actions
            where sym in syms, action_type=`split,
                  ex_date > startDate, ex_date <= endDate;
        if[count splitEvts;
            .lg.o[`adjlib;"split boundary continuity:"];
            {[e;result]
                prevClose:exec last  adj_close from result where sym=e`sym, date <  e`ex_date;
                postClose:exec first adj_close from result where sym=e`sym, date >= e`ex_date;
                if[(not null prevClose) and not null postClose;
                    .lg.o[`adjlib;
                        "  ",string[e`sym]," ",string[e`ex_date],
                        ": pre_adj_close=",string[prevClose],
                        " post_adj_close=",string[postClose],
                        " diff=",string[abs prevClose-postClose]]
                ]
            }[;result] each splitEvts
        ]
    ];

    result
 };
