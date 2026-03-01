import streamlit as st
from snowflake.snowpark.context import get_active_session
import pandas as pd
from datetime import datetime, timedelta

session = get_active_session()

# --- Custom CSS ---
st.markdown("""
<style>
/* Wide layout */
.block-container {
    max-width: 95% !important;
    padding-left: 2rem !important;
    padding-right: 2rem !important;
}

/* Header bar */
.main-header {
    background: linear-gradient(135deg, #2563eb 0%, #3b82f6 50%, #60a5fa 100%);
    padding: 1.5rem 2rem;
    border-radius: 10px;
    margin-bottom: 1rem;
    color: white;
}
.main-header h1 {
    margin: 0;
    font-size: 2rem;
    font-weight: 700;
    letter-spacing: 0.5px;
    color: white;
    text-shadow: 0 1px 3px rgba(0,0,0,0.3);
}
.main-header p {
    margin: 0.3rem 0 0 0;
    opacity: 0.85;
    font-size: 0.9rem;
    color: white;
}

/* Search input box styling */
[data-testid="stTextInput"] input {
    background: white !important;
    border: 2px solid #3b82f6 !important;
    border-radius: 8px !important;
    padding: 0.6rem 1rem !important;
    font-size: 1rem !important;
    box-shadow: 0 2px 6px rgba(59,130,246,0.15) !important;
}
[data-testid="stTextInput"] input:focus {
    border-color: #2563eb !important;
    box-shadow: 0 2px 10px rgba(37,99,235,0.3) !important;
}

/* Severity badge styles */
.sev-badge {
    display: inline-block;
    padding: 2px 10px;
    border-radius: 12px;
    font-size: 0.75rem;
    font-weight: 700;
    letter-spacing: 0.5px;
    color: white;
}
.sev-fatal { background: #dc2626; }
.sev-error { background: #ea580c; }
.sev-warn  { background: #ca8a04; }
.sev-info  { background: #2563eb; }
.sev-debug { background: #6b7280; }

/* Metric cards with colored top border */
[data-testid="stMetricValue"] {
    font-size: 1.8rem;
    font-weight: 700;
}

/* Detail card */
.detail-card {
    background: #f8fafc;
    border-left: 4px solid #3b82f6;
    padding: 0.8rem 1rem;
    border-radius: 0 6px 6px 0;
    margin-bottom: 0.5rem;
    font-family: monospace;
    font-size: 0.85rem;
}
.detail-card.fatal { border-left-color: #dc2626; }
.detail-card.error { border-left-color: #ea580c; }
.detail-card.warn  { border-left-color: #ca8a04; }
.detail-card.info  { border-left-color: #2563eb; }
.detail-card.debug { border-left-color: #6b7280; }

.detail-meta {
    color: #64748b;
    font-size: 0.78rem;
    margin-top: 0.3rem;
}
</style>
""", unsafe_allow_html=True)

# --- Header ---
st.markdown("""
<div class="main-header">
    <h1>Keyword Search</h1>
    <p>Snowflake SEARCH() + Search Optimization Service</p>
</div>
""", unsafe_allow_html=True)

# --- Sidebar Filters ---
with st.sidebar:
    st.header("Filters")

    # Time range
    st.subheader("Time Range")
    time_preset = st.selectbox(
        "Quick select",
        ["Last 1 hour", "Last 6 hours", "Last 24 hours", "Last 7 days", "Last 30 days", "Last 3 months", "Last 1 year", "Last 3 years", "Custom"],
        index=4,
    )

    now = datetime.now()
    if time_preset == "Last 1 hour":
        start_time = now - timedelta(hours=1)
        end_time = now
    elif time_preset == "Last 6 hours":
        start_time = now - timedelta(hours=6)
        end_time = now
    elif time_preset == "Last 24 hours":
        start_time = now - timedelta(hours=24)
        end_time = now
    elif time_preset == "Last 7 days":
        start_time = now - timedelta(days=7)
        end_time = now
    elif time_preset == "Last 30 days":
        start_time = now - timedelta(days=30)
        end_time = now
    elif time_preset == "Last 3 months":
        start_time = now - timedelta(days=90)
        end_time = now
    elif time_preset == "Last 1 year":
        start_time = now - timedelta(days=365)
        end_time = now
    elif time_preset == "Last 3 years":
        start_time = now - timedelta(days=1095)
        end_time = now
    else:
        col_s, col_e = st.columns(2)
        with col_s:
            start_date = st.date_input("Start", value=now - timedelta(days=7))
        with col_e:
            end_date = st.date_input("End", value=now)
        start_time = datetime.combine(start_date, datetime.min.time())
        end_time = datetime.combine(end_date, datetime.max.time())

    # Severity filter
    st.subheader("Severity")
    severities = st.multiselect(
        "Select severity levels",
        ["FATAL", "ERROR", "WARN", "INFO", "DEBUG"],
        default=["FATAL", "ERROR", "WARN", "INFO", "DEBUG"],
    )

    # Source filter
    sources_df = session.sql(
        "SELECT DISTINCT SOURCE FROM LOG_SEARCH_APP.PUBLIC.LOGS ORDER BY SOURCE"
    ).to_pandas()
    all_sources = sources_df["SOURCE"].tolist()

    st.subheader("Source")
    selected_sources = st.multiselect(
        "Select sources",
        all_sources,
        default=all_sources,
    )

    # Search mode
    st.subheader("Search Mode")
    search_mode = st.radio(
        "Match type",
        ["OR", "AND", "PHRASE"],
        index=0,
        help="OR: any keyword matches. AND: all keywords must match. PHRASE: exact phrase match.",
    )

    # Max results
    max_results = st.slider("Max results", 1000, 10000000, 10000, step=1000)

    # --- Search Optimization Management ---
    st.markdown("---")
    st.subheader("Search Optimization")

    TABLE_FQN = "LOG_SEARCH_APP.PUBLIC.LOGS"

    def get_so_status():
        try:
            result = session.sql(
                f"DESCRIBE SEARCH OPTIMIZATION ON {TABLE_FQN}"
            ).to_pandas()
            return result
        except Exception:
            return None

    so_status = get_so_status()

    if so_status is not None and len(so_status) > 0:
        st.success("Status: Configured")

        # Show target columns
        for _, r in so_status.iterrows():
            method = r.get('"method"', "N/A")
            target = r.get('"target"', "N/A")
            st.caption(f"Target: `{method}` on `{target}`")

        if st.button("Disable Search Optimization"):
            try:
                session.sql(
                    f"ALTER TABLE {TABLE_FQN} DROP SEARCH OPTIMIZATION"
                ).collect()
                st.warning("Search Optimization has been disabled. Please reload the page.")
            except Exception as e:
                st.error(f"Failed to disable: {e}")

        if st.button("Check Index Status"):
            so_check = get_so_status()
            if so_check is not None:
                for _, r in so_check.iterrows():
                    active = r.get('"active"', "unknown")
                    method = r.get('"method"', "N/A")
                    target = r.get('"target"', "N/A")
                    if active == "true" or active is True:
                        st.success(f"Index READY - {method} on `{target}`")
                    else:
                        st.warning(f"Index BUILDING - {method} on `{target}`")
                st.dataframe(so_check, use_container_width=True)
            else:
                st.info("No search optimization found.")
    else:
        st.info("Status: Not configured")

        if st.button("Enable Search Optimization"):
            try:
                session.sql(
                    f"ALTER TABLE {TABLE_FQN} ADD SEARCH OPTIMIZATION "
                    f"ON FULL_TEXT(MESSAGE, ANALYZER => 'UNICODE_ANALYZER')"
                ).collect()
                st.success("Search Optimization enabled. Indexing will start in the background.")
            except Exception as e:
                st.error(f"Failed to enable: {e}")

    # --- Warehouse Size Management ---
    st.markdown("---")
    st.subheader("Warehouse")

    WH_NAME = "SEARCH_WH"
    WH_SIZES = ["X-Small", "Small", "Medium", "Large", "X-Large", "2X-Large", "3X-Large", "4X-Large"]
    WH_SIZE_MAP = {
        "X-Small": "XSMALL", "Small": "SMALL", "Medium": "MEDIUM", "Large": "LARGE",
        "X-Large": "XLARGE", "2X-Large": "XXLARGE", "3X-Large": "XXXLARGE", "4X-Large": "X4LARGE",
    }

    try:
        wh_info = session.sql(f"SHOW WAREHOUSES LIKE '{WH_NAME}'").to_pandas()
        if len(wh_info) > 0:
            # SiS returns column names with double quotes
            if '"size"' in wh_info.columns:
                current_size = wh_info['"size"'].iloc[0]
            elif 'size' in wh_info.columns:
                current_size = wh_info['size'].iloc[0]
            else:
                current_size = "Unknown"
        else:
            current_size = "Unknown"
        st.info(f"Name: **{WH_NAME}** | Size: **{current_size}**")
    except Exception:
        current_size = "Unknown"
        st.warning("Could not retrieve warehouse info.")

    new_size = st.selectbox("Change size", WH_SIZES, index=0)

    if st.button("Apply Warehouse Size"):
        try:
            session.sql(
                f"ALTER WAREHOUSE {WH_NAME} SET WAREHOUSE_SIZE = '{WH_SIZE_MAP[new_size]}'"
            ).collect()
            st.success(f"Warehouse resized to **{new_size}**. Please reload the page.")
        except Exception as e:
            st.error(f"Failed to resize: {e}")

# --- Search Bar ---
search_query = st.text_input(
    "キーワードを入力して検索",
    placeholder="例: timeout error, OutOfMemory, 503 ...",
)

search_clicked = st.button("検索")

# --- Total Record Count ---
total_records = session.sql(
    "SELECT COUNT(*) AS CNT FROM LOG_SEARCH_APP.PUBLIC.LOGS"
).to_pandas()["CNT"][0]
st.caption(f"対象テーブル: `LOG_SEARCH_APP.PUBLIC.LOGS` — 総レコード数: **{int(total_records):,}** 件")

# --- Raw Data Preview ---
with st.expander("元データを確認"):
    preview_limit = st.number_input("表示件数", min_value=1, max_value=10000, value=100, step=100)
    raw_df = session.sql(
        f"SELECT * FROM LOG_SEARCH_APP.PUBLIC.LOGS ORDER BY TIMESTAMP DESC LIMIT {int(preview_limit)}"
    ).to_pandas()
    st.dataframe(raw_df, use_container_width=True)

# --- Build & Execute Query ---
def build_query(search_text, severities, sources, start, end, mode, limit):
    conditions = []
    params = []

    # Time range
    conditions.append("TIMESTAMP BETWEEN ? AND ?")
    params.append(start)
    params.append(end)

    # Severity
    if severities and len(severities) < 5:
        placeholders = ", ".join(["?"] * len(severities))
        conditions.append(f"SEVERITY IN ({placeholders})")
        params.extend(severities)

    # Source
    if sources and len(sources) < len(all_sources):
        placeholders = ", ".join(["?"] * len(sources))
        conditions.append(f"SOURCE IN ({placeholders})")
        params.extend(sources)

    # Full-text search
    if search_text and search_text.strip():
        conditions.append(
            f"SEARCH((*), ?, SEARCH_MODE => '{mode}', ANALYZER => 'UNICODE_ANALYZER')"
        )
        params.append(search_text.strip())

    where_clause = " AND ".join(conditions)
    query = f"""
        SELECT LOG_ID, TIMESTAMP, SEVERITY, SOURCE, HOST, MESSAGE
        FROM LOG_SEARCH_APP.PUBLIC.LOGS
        WHERE {where_clause}
        ORDER BY TIMESTAMP DESC
        LIMIT {int(limit)}
    """
    return query, params


if search_clicked:
    query, params = build_query(
        search_query, severities, selected_sources, start_time, end_time, search_mode, max_results
    )

    df = session.sql(query, params=params).to_pandas()

    # --- Summary Metrics with severity color badges ---
    total = len(df)
    fatal_count = len(df[df["SEVERITY"] == "FATAL"]) if total > 0 else 0
    error_count = len(df[df["SEVERITY"] == "ERROR"]) if total > 0 else 0
    warn_count = len(df[df["SEVERITY"] == "WARN"]) if total > 0 else 0
    info_count = len(df[df["SEVERITY"] == "INFO"]) if total > 0 else 0
    debug_count = len(df[df["SEVERITY"] == "DEBUG"]) if total > 0 else 0

    m1, m2, m3, m4, m5, m6 = st.columns(6)
    m1.metric("Total", f"{total:,}")
    m2.markdown(f'<span class="sev-badge sev-fatal">FATAL</span>', unsafe_allow_html=True)
    m2.metric("FATAL", f"{fatal_count:,}")
    m3.markdown(f'<span class="sev-badge sev-error">ERROR</span>', unsafe_allow_html=True)
    m3.metric("ERROR", f"{error_count:,}")
    m4.markdown(f'<span class="sev-badge sev-warn">WARN</span>', unsafe_allow_html=True)
    m4.metric("WARN", f"{warn_count:,}")
    m5.markdown(f'<span class="sev-badge sev-info">INFO</span>', unsafe_allow_html=True)
    m5.metric("INFO", f"{info_count:,}")
    m6.markdown(f'<span class="sev-badge sev-debug">DEBUG</span>', unsafe_allow_html=True)
    m6.metric("DEBUG", f"{debug_count:,}")

    # --- Tabbed Layout ---
    df["TIMESTAMP"] = pd.to_datetime(df["TIMESTAMP"]) if total > 0 else df["TIMESTAMP"]

    tab_charts, tab_events, tab_details = st.tabs(["Charts", "Events", "Details"])

    # ===== Charts Tab =====
    with tab_charts:
        if total > 0:
            col_chart, col_sources = st.columns([2, 1])

            with col_chart:
                st.subheader("Event Timeline")
                df["hour_bucket"] = df["TIMESTAMP"].dt.floor("h")
                timeline_df = (
                    df.groupby(["hour_bucket", "SEVERITY"])
                    .size()
                    .reset_index(name="count")
                )
                timeline_pivot = timeline_df.pivot_table(
                    index="hour_bucket", columns="SEVERITY", values="count", fill_value=0
                ).reset_index()
                timeline_pivot = timeline_pivot.rename(columns={"hour_bucket": "Time"})
                timeline_pivot = timeline_pivot.set_index("Time")
                sev_order = [s for s in ["FATAL", "ERROR", "WARN", "INFO", "DEBUG"] if s in timeline_pivot.columns]
                timeline_pivot = timeline_pivot[sev_order]
                st.bar_chart(timeline_pivot)

            with col_sources:
                st.subheader("Top Sources")
                source_counts = df["SOURCE"].value_counts().reset_index()
                source_counts.columns = ["Source", "Count"]
                st.bar_chart(source_counts.set_index("Source"))

            # --- Row 2: By Severity + Events by Host ---
            col_sev, col_host = st.columns(2)

            with col_sev:
                st.subheader("By Severity")
                sev_counts = df["SEVERITY"].value_counts().reset_index()
                sev_counts.columns = ["Severity", "Count"]
                st.dataframe(sev_counts, use_container_width=True)

            with col_host:
                st.subheader("Events by Host")
                host_counts = df["HOST"].value_counts().reset_index()
                host_counts.columns = ["Host", "Count"]
                host_chart = host_counts.head(15).set_index("Host")
                st.bar_chart(host_chart)

            # --- Row 3: Extracted Fields ---
            st.markdown("---")
            st.subheader("Extracted Fields")
            st.caption("MESSAGEカラムから自動抽出されたフィールドの値分布（出現頻度順・上位15フィールド）")

            # --- Generic key=value parser ---
            all_kvs = df["MESSAGE"].str.extractall(r'([a-z_]+)=(\S+)')
            extracted = {}
            if len(all_kvs) > 0:
                all_kvs.columns = ["key", "value"]
                for key, grp in all_kvs.groupby("key"):
                    top_vals = grp["value"].value_counts().head(10).reset_index()
                    top_vals.columns = ["Value", "Count"]
                    extracted[key] = top_vals

            # --- Supplemental patterns for non key=value fields ---
            special_patterns = {
                "http_status": r'HTTP\s(\d{3})',
                "timeout_ms": r'after\s(\d+)ms',
                "retry_attempt": r'attempt\s(\d+)',
            }
            for field_name, pattern in special_patterns.items():
                if field_name not in extracted:
                    vals = df["MESSAGE"].str.extract(pattern, expand=False).dropna()
                    if len(vals) > 0:
                        top_vals = vals.value_counts().head(10).reset_index()
                        top_vals.columns = ["Value", "Count"]
                        extracted[field_name] = top_vals

            if extracted:
                # Sort fields by total occurrence count (descending), limit to top 15
                sorted_fields = sorted(
                    extracted.keys(),
                    key=lambda k: extracted[k]["Count"].sum(),
                    reverse=True,
                )[:15]

                # Display in 3-column layout
                for i in range(0, len(sorted_fields), 3):
                    cols_ef = st.columns(3)
                    for j in range(3):
                        idx = i + j
                        if idx < len(sorted_fields):
                            fname = sorted_fields[idx]
                            with cols_ef[j]:
                                st.markdown(f"**{fname}**")
                                st.dataframe(extracted[fname], use_container_width=True)
            else:
                st.caption("抽出可能なフィールドが見つかりませんでした。")
        else:
            st.caption("No log events found. Try adjusting your search query or filters.")

    # ===== Events Tab =====
    with tab_events:
        if total > 0:
            st.subheader(f"Log Events ({total:,} results)")
            display_df = df[["TIMESTAMP", "SEVERITY", "SOURCE", "HOST", "MESSAGE"]].copy()
            display_df["TIMESTAMP"] = display_df["TIMESTAMP"].dt.strftime("%Y-%m-%d %H:%M:%S")
            display_df.columns = ["Time", "Severity", "Source", "Host", "Message"]
            st.dataframe(display_df, use_container_width=True)
        else:
            st.caption("No log events found. Try adjusting your search query or filters.")

    # ===== Details Tab =====
    with tab_details:
        if total > 0:
            st.subheader("Log Details")
            st.caption("Expand a row to see the full log message:")
            for _, row in df.head(30).iterrows():
                sev = row["SEVERITY"]
                sev_class = sev.lower()
                ts = row["TIMESTAMP"].strftime("%Y-%m-%d %H:%M:%S")
                badge = f'<span class="sev-badge sev-{sev_class}">{sev}</span>'
                with st.expander(f"[{sev}] {ts} | {row['SOURCE']} | {str(row['MESSAGE'])[:80]}"):
                    st.markdown(
                        f'<div class="detail-card {sev_class}">'
                        f'{badge} <strong>{ts}</strong>'
                        f'<pre style="white-space:pre-wrap;margin:0.5rem 0;">{row["MESSAGE"]}</pre>'
                        f'<div class="detail-meta">'
                        f'Host: <code>{row["HOST"]}</code> &nbsp; '
                        f'Source: <code>{row["SOURCE"]}</code> &nbsp; '
                        f'Log ID: <code>{row["LOG_ID"]}</code>'
                        f'</div></div>',
                        unsafe_allow_html=True
                    )
        else:
            st.caption("No log events found. Try adjusting your search query or filters.")

# ===== Help (always visible) =====
st.markdown("---")
st.subheader("このアプリについて")
st.markdown("""
Snowflake の **SEARCH() 関数** と **Search Optimization Service** を活用した、
Splunk ライクなログ全文検索アプリケーションです。
""")

st.subheader("機能一覧")
st.markdown("""
| 機能 | 説明 |
|---|---|
| **全文検索** | Snowflake `SEARCH()` 関数によるトークンベースのテキスト検索 |
| **検索モード** | OR（いずれかのキーワード）、AND（全キーワード一致）、PHRASE（完全フレーズ一致） |
| **時間範囲フィルタ** | プリセット（1時間〜30日）またはカスタム日付範囲 |
| **重要度フィルタ** | FATAL / ERROR / WARN / INFO / DEBUG レベルでの絞り込み |
| **ソースフィルタ** | アプリケーション・サービス名での絞り込み |
| **タイムライン棒グラフ** | 時間帯×重要度別のイベント件数ヒストグラム |
| **Top Sources** | ソース別ヒット数のランキングチャート |
| **By Severity** | 重要度別の件数テーブル |
| **Events by Host** | ホスト別イベント数の棒グラフ（上位15件） |
| **ログ詳細ビュー** | 各ログの全文メッセージとメタデータを展開表示 |
| **Search Optimization管理** | サイドバーからインデックスの有効化・無効化・状態確認が可能 |
""")

st.subheader("使い方")
st.markdown("""
**1. 検索する**

ページ上部の検索バーにキーワードを入力し、「検索」ボタンを押します。
複数の単語はサイドバーで選択した検索モードに従って検索されます：

- **OR**（デフォルト） - いずれかのキーワードを含むログを返します。
  例: `timeout error` → "timeout" または "error" を含むログがヒット
- **AND** - すべてのキーワードを含むログのみ返します。
  例: `timeout database` → 両方の単語を含むログのみヒット
- **PHRASE** - 入力した通りの完全なフレーズを検索します。
  例: `Connection timeout` → この語順で連続する箇所のみヒット

**2. フィルタで絞り込む**

サイドバーのフィルタを使って結果を絞り込みます：

- **Time Range** - プリセット（1時間〜30日）またはカスタム日付範囲を選択
- **Severity** - 表示する重要度レベルのチェックを切り替え
- **Source** - 検索対象のアプリケーション・サービスを選択
- **Max Results** - 返却する最大件数を調整

**3. 結果を確認する**

タブを切り替えてデータを探索します：

- **Charts** - 時間帯ごとのイベント量の棒グラフ、ソース別ランキング、
  重要度別件数テーブル、ホスト別イベント数チャートを表示。
- **Events** - 時刻・重要度・ソース・ホスト・メッセージのデータテーブル。
  列ヘッダーをクリックしてソート可能。
- **Details** - 個別ログの展開リスト。クリックするとメッセージ全文と
  メタデータ（ホスト・ソース・ログID）を表示。

**4. Search Optimization（サイドバー）**

Snowflake Search Optimization Service をサイドバーから管理できます：

- **Check Index Status** - インデックスの構築状態を確認。
  "Index READY" は最適化済み、"Index BUILDING" は構築中（検索は動作するが低速の可能性あり）。
- **Enable** - MESSAGE カラムに FULL_TEXT インデックスを作成（UNICODE_ANALYZER使用）。
- **Disable** - Search Optimization インデックスを削除。
""")

st.subheader("技術情報")
st.markdown("""
- **対象テーブル**: `LOG_SEARCH_APP.PUBLIC.LOGS`
- **検索関数**: `SEARCH((*), ?, SEARCH_MODE => '...', ANALYZER => 'UNICODE_ANALYZER')`
- **インデックス種類**: Search Optimization Service（`FULL_TEXT` メソッド）
- **アナライザー**: `UNICODE_ANALYZER`（大文字小文字を区別しない、Unicode対応のトークン分割）
""")
