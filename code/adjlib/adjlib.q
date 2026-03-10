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
// ---------------------------------------------------------------------------
applyAdj:{[ohlcvData;factors]
    // Build a keyed lookup table: (sym,date) -> cumulative_factor only
    kf:`sym`date xkey select sym,date,cumulative_factor from factors;

    // Left join: each bar gets its factor for that exact (sym,date).
    // Bars whose date is not in the factor table get a null factor.
    joined:ohlcvData lj kf;

    // Fill missing factors with 1.0 so unadjusted bars pass through unchanged
    joined:update cumulative_factor:1.0 from joined where null cumulative_factor;

    // Vectorised price adjustment (multiply) and volume adjustment (divide)
    adjusted:update
        open:  open  * cumulative_factor,
        high:  high  * cumulative_factor,
        low:   low   * cumulative_factor,
        close: close * cumulative_factor,
        volume:`long$volume % cumulative_factor
    from joined;

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
//
// Returns the ohlcv_1m columns plus adj_close (the adjusted close price).
// When ref_adj_factors has no entry for a (sym,date), adj_close equals close.
// ---------------------------------------------------------------------------
getAdjustedClose:{[syms;startDate;endDate;method]
    syms:$[-11h=type syms; enlist syms; syms];
    if[not method in `backward`forward;
        '"unsupported method: ",string method
    ];

    // Raw bars from HDB (or in-memory ohlcv_1m if HDB not loaded in tests)
    bars:select from ohlcv_1m where date within (startDate;endDate), sym in syms;

    // Factors for the requested syms/dates from the in-memory ref table
    factors:select sym,date,cumulative_factor from ref_adj_factors
        where sym in syms, date within (startDate;endDate);

    if[0=count factors;
        .lg.o[`adjlib;"no adjustment factors found for ",
              (", " sv string syms)," — returning unadjusted prices"]
    ];

    // Apply backward (post-split) price and volume adjustments — common to both methods
    adjBars:applyAdj[bars;factors];

    $[method=`backward;
        // backward: all prices in post-split (current) terms.
        // adj_close is the backward-adjusted close (same as the `close` column).
        update adj_close:close from adjBars;
        // forward: all prices in pre-split (historical) terms.
        // Divide each bar's backward-adjusted price by the first cumulative_factor
        // for that sym within the requested date range.  This normalises all bars
        // to the price scale that was in effect at the start of the window.
        [firstF:select first_factor:first cumulative_factor by sym
                 from `date xasc factors;
         adjFwd:adjBars lj firstF;
         adjFwd:update first_factor:1.0 from adjFwd where null first_factor;
         adjFwd:update
             open:   open  % first_factor,
             high:   high  % first_factor,
             low:    low   % first_factor,
             close:  close % first_factor,
             volume: `long$(volume * first_factor)
         from adjFwd;
         (cols[ohlcv_1m],`adj_close) # update adj_close:close from adjFwd]
     ]
 };
