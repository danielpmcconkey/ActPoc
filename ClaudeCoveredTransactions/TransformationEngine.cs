using Npgsql;

namespace ClaudeCoveredTransactions;

// Implements Functional Spec Sections 4.2 through 4.6 — Transformation Pipeline
//
// All filtering, joining, enrichment, deduplication, and sorting are performed
// in a single SQL query. No row-by-row iteration. Set-based per Spec Section 7.3.
//
// Pipeline steps implemented in SQL:
//   Step 2: Select transactions for resolved as_of (BR-2)
//   Step 3: Join accounts, filter to Checking (BR-3)
//   Step 4: Join addresses, filter to active US, tie-break earliest start_date (BR-4, BR-5)
//   Step 5: Join customers for enrichment (BR-6, BR-8, BR-10)
//   Step 6: Join segments via customers_segments, dedup alphabetical first (BR-6, BR-7)
//   Sort:   customer_id ASC, transaction_id DESC (BR-14)

public static class TransformationEngine
{
    public static List<CoveredTransaction> Execute(
        ResolvedSnapshots snapshots,
        DateTime effectiveDate)
    {
        // The entire transformation is a single SQL statement.
        // This satisfies the set-based requirement (Spec Section 7.3) and ensures
        // the database engine handles all joins, filters, and dedup operations
        // at scale (5M customers, 10M accounts, 20M transactions per Spec 1.4).
        //
        // Address tie-breaking (BR-5): ROW_NUMBER() partitioned by customer_id,
        //   ordered by start_date ASC, selects the earliest start_date.
        //   If start_dates are equal, address_id ASC breaks the tie deterministically.
        //
        // Segment dedup (BR-7): DISTINCT ON or MIN(segment_code) after joining
        //   customers_segments -> segments, grouped by customer_id.

        string sql = @"
            -- Step 4: Resolve active US addresses with tie-breaking (BR-4, BR-5)
            -- Implements: country = 'US', end_date IS NULL OR end_date >= effective_date
            -- Tie-break: earliest start_date; if equal, lowest address_id for determinism
            WITH active_us_addresses AS (
                SELECT
                    a.customer_id,
                    a.address_id,
                    a.address_line1,
                    a.city,
                    a.state_province,
                    a.postal_code,
                    a.country,
                    ROW_NUMBER() OVER (
                        PARTITION BY a.customer_id
                        ORDER BY a.start_date ASC, a.address_id ASC
                    ) AS rn
                FROM public.addresses a
                WHERE a.as_of = @AddressesAsOf
                  AND a.country = 'US'
                  AND (a.end_date IS NULL OR a.end_date >= @EffectiveDate)
            ),

            -- Step 6: Resolve customer segments with dedup (BR-6, BR-7)
            -- Implements: join customers_segments to segments, then pick first segment_code
            -- alphabetically per customer. Duplicate assignments are collapsed by DISTINCT.
            customer_segment_resolved AS (
                SELECT
                    cs.customer_id,
                    MIN(s.segment_code) AS segment_code
                FROM public.customers_segments cs
                INNER JOIN public.segments s
                    ON cs.segment_id = s.segment_id
                   AND s.as_of = @SegmentsAsOf
                WHERE cs.as_of = @CustomersSegmentsAsOf
                GROUP BY cs.customer_id
            )

            -- Main query: Steps 2, 3, 5 + assembly from CTEs
            SELECT
                -- Pos 1-5: Transaction fields (Step 2, BR-2)
                t.transaction_id,
                t.txn_timestamp,
                t.txn_type,
                t.amount,
                t.description,

                -- Pos 6: customer_id from accounts (Step 3)
                acct.customer_id,

                -- Pos 7-11: Customer fields (Step 5, BR-6, BR-8, BR-10)
                -- prefix -> name_prefix, suffix -> name_suffix (BR-8 renames)
                -- birthdate excluded (BR-10)
                cust.prefix AS name_prefix,
                cust.first_name,
                cust.last_name,
                cust.sort_name,
                cust.suffix AS name_suffix,

                -- Pos 12: Segment enrichment (Step 6, BR-6, BR-7)
                csr.segment_code AS customer_segment,

                -- Pos 13-18: Address fields (Step 4, BR-4, BR-5)
                -- start_date and end_date excluded (BR-11)
                addr.address_id,
                addr.address_line1,
                addr.city,
                addr.state_province,
                addr.postal_code,
                addr.country,

                -- Pos 19-22: Account fields (Step 3, BR-9)
                -- current_balance, interest_rate, credit_limit, apr excluded (BR-9)
                -- open_date -> account_opened (BR-8 rename)
                acct.account_id,
                acct.account_type,
                acct.account_status,
                acct.open_date AS account_opened

            -- Step 2: Select transactions for resolved as_of (BR-2)
            FROM public.transactions t

            -- Step 3: Join accounts, filter to Checking (BR-3)
            -- account_status is NOT a filter (BR-3 confirmed)
            INNER JOIN public.accounts acct
                ON t.account_id = acct.account_id
               AND acct.as_of = @AccountsAsOf
               AND acct.account_type = 'Checking'

            -- Step 4: Join active US address (BR-4, BR-5)
            -- Inner join: customers without active US address are excluded
            INNER JOIN active_us_addresses addr
                ON acct.customer_id = addr.customer_id
               AND addr.rn = 1

            -- Step 5: Join customers for enrichment (BR-6)
            -- Inner join: transactions without matching customer are excluded
            INNER JOIN public.customers cust
                ON acct.customer_id = cust.id
               AND cust.as_of = @CustomersAsOf

            -- Step 6: Join resolved segments (BR-6, BR-7)
            -- Inner join: customers without segment assignment are excluded (Spec Section 6.3)
            INNER JOIN customer_segment_resolved csr
                ON acct.customer_id = csr.customer_id

            WHERE t.as_of = @TransactionsAsOf

            -- Step 7 (partial): Sort order (BR-14)
            ORDER BY acct.customer_id ASC, t.transaction_id DESC
        ";

        using var conn = DataAccessLayer.GetConnection();
        conn.Open();
        using var cmd = new NpgsqlCommand(sql, conn);

        cmd.Parameters.AddWithValue("@TransactionsAsOf", snapshots.Transactions);
        cmd.Parameters.AddWithValue("@AccountsAsOf", snapshots.Accounts);
        cmd.Parameters.AddWithValue("@CustomersAsOf", snapshots.Customers);
        cmd.Parameters.AddWithValue("@AddressesAsOf", snapshots.Addresses);
        cmd.Parameters.AddWithValue("@CustomersSegmentsAsOf", snapshots.CustomersSegments);
        cmd.Parameters.AddWithValue("@SegmentsAsOf", snapshots.Segments);
        cmd.Parameters.AddWithValue("@EffectiveDate", effectiveDate);

        using var reader = cmd.ExecuteReader();
        var results = new List<CoveredTransaction>();

        while (reader.Read())
        {
            results.Add(new CoveredTransaction(
                transaction_id:   reader.GetInt32(0),
                txn_timestamp:    reader.GetDateTime(1),
                txn_type:         reader.GetString(2),
                amount:           reader.GetDecimal(3),
                description:      reader.IsDBNull(4) ? null : reader.GetString(4),
                customer_id:      reader.GetInt32(5),
                name_prefix:      reader.IsDBNull(6) ? null : reader.GetString(6),
                first_name:       reader.GetString(7),
                last_name:        reader.GetString(8),
                sort_name:        reader.GetString(9),
                name_suffix:      reader.IsDBNull(10) ? null : reader.GetString(10),
                customer_segment: reader.GetString(11),
                address_id:       reader.GetInt32(12),
                address_line1:    reader.GetString(13),
                city:             reader.GetString(14),
                state_province:   reader.GetString(15),
                postal_code:      reader.GetString(16),
                country:          reader.GetString(17),
                account_id:       reader.GetInt32(18),
                account_type:     reader.GetString(19),
                account_status:   reader.GetString(20),
                account_opened:   reader.GetDateTime(21)
            ));
        }

        return results;
    }
}
