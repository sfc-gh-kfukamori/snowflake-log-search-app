import streamlit as st
from snowflake.core import Root
from snowflake.snowpark.context import get_active_session
import pandas as pd

session = get_active_session()
root = Root(session)

# --- Constants ---
DB = "LOG_SEARCH_APP"
SCHEMA = "PUBLIC"
SERVICE = "LOG_SEMANTIC_SEARCH"

# --- Custom CSS (same style as main page) ---
st.markdown("""
<style>
/* Wide layout */
.block-container {
    max-width: 95% !important;
    padding-left: 2rem !important;
    padding-right: 2rem !important;
}

.main-header {
    background: linear-gradient(135deg, #7c3aed 0%, #8b5cf6 50%, #a78bfa 100%);
    padding: 1.5rem 2rem;
    border-radius: 10px;
    margin-bottom: 1rem;
    color: white;
}
.main-header h1 {
    margin: 0; font-size: 2rem; font-weight: 700;
    letter-spacing: 0.5px; color: white;
    text-shadow: 0 1px 3px rgba(0,0,0,0.3);
}
.main-header p {
    margin: 0.3rem 0 0 0; opacity: 0.85;
    font-size: 0.9rem; color: white;
}

[data-testid="stTextInput"] input {
    background: white !important;
    border: 2px solid #8b5cf6 !important;
    border-radius: 8px !important;
    padding: 0.6rem 1rem !important;
    font-size: 1rem !important;
    box-shadow: 0 2px 6px rgba(139,92,246,0.15) !important;
}
[data-testid="stTextInput"] input:focus {
    border-color: #7c3aed !important;
    box-shadow: 0 2px 10px rgba(124,58,237,0.3) !important;
}

.sev-badge {
    display: inline-block; padding: 2px 10px; border-radius: 12px;
    font-size: 0.75rem; font-weight: 700; letter-spacing: 0.5px; color: white;
}
.sev-fatal { background: #dc2626; }
.sev-error { background: #ea580c; }
.sev-warn  { background: #ca8a04; }
.sev-info  { background: #2563eb; }
.sev-debug { background: #6b7280; }

.result-card {
    background: #f8fafc;
    border-left: 4px solid #8b5cf6;
    padding: 0.8rem 1rem;
    border-radius: 0 6px 6px 0;
    margin-bottom: 0.5rem;
    font-family: monospace;
    font-size: 0.85rem;
}
.result-card.fatal { border-left-color: #dc2626; }
.result-card.error { border-left-color: #ea580c; }
.result-card.warn  { border-left-color: #ca8a04; }
.result-card.info  { border-left-color: #2563eb; }
.result-card.debug { border-left-color: #6b7280; }

.result-meta {
    color: #64748b; font-size: 0.78rem; margin-top: 0.3rem;
}

.ai-analysis {
    background: linear-gradient(135deg, #f5f3ff 0%, #ede9fe 100%);
    border: 1px solid #c4b5fd;
    border-radius: 10px;
    padding: 1.2rem 1.5rem;
    margin-top: 1rem;
}
.ai-analysis h3 {
    color: #5b21b6;
    margin-top: 0;
}
</style>
""", unsafe_allow_html=True)

# --- Header ---
st.markdown("""<div class="main-header">
    <h1>Semantic Log Search</h1>
    <p>Cortex Search Service - AI-powered semantic search</p>
</div>""", unsafe_allow_html=True)

# --- Sidebar Filters ---
with st.sidebar:
    st.header("Semantic Search Filters")

    # Severity filter
    st.subheader("Severity")
    sev_filter = st.multiselect(
        "Filter by severity",
        ["FATAL", "ERROR", "WARN", "INFO", "DEBUG"],
        default=[],
        help="Leave empty to search all severities",
    )

    # Source filter
    sources_df = session.sql(
        "SELECT DISTINCT SOURCE FROM LOG_SEARCH_APP.PUBLIC.LOGS_SMALL ORDER BY SOURCE"
    ).to_pandas()
    all_sources = sources_df["SOURCE"].tolist()

    st.subheader("Source")
    src_filter = st.multiselect(
        "Filter by source",
        all_sources,
        default=[],
        help="Leave empty to search all sources",
    )

    # Max results
    max_results = st.slider("Max results", 1, 1000, 10)

    # Service status
    st.markdown("---")
    st.subheader("Service Status")
    try:
        svc_info = session.sql(
            f"SHOW CORTEX SEARCH SERVICES LIKE '{SERVICE}' IN SCHEMA {DB}.{SCHEMA}"
        ).to_pandas()
        if len(svc_info) > 0:
            # SiS returns column names with double quotes
            def _col(df, name):
                if f'"{name}"' in df.columns:
                    return df[f'"{name}"'].iloc[0]
                elif name in df.columns:
                    return df[name].iloc[0]
                return "Unknown"
            serving = _col(svc_info, "serving_state")
            rows = _col(svc_info, "source_data_num_rows")
            st.info(f"Serving: **{serving}**")
            st.info(f"Indexed rows: **{int(rows):,}**")
        else:
            st.warning("Service not found.")
    except Exception as e:
        st.warning(f"Could not check status: {e}")

    # --- Warehouse ---
    st.markdown("---")
    st.subheader("Warehouse")
    WH_NAME = "SEARCH_WH"
    try:
        wh_info = session.sql(f"SHOW WAREHOUSES LIKE '{WH_NAME}'").to_pandas()
        if len(wh_info) > 0:
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
        st.warning("Could not retrieve warehouse info.")

# --- Search Bar ---
search_query = st.text_input(
    "セマンティック検索（自然言語で入力）",
    placeholder="例: メモリ不足でサービスが停止した, データベース接続に失敗 ...",
)

search_clicked = st.button("検索")

st.caption(
    "Cortex Search はキーワード一致ではなく、意味的に類似したログを検索します。"
    "自然言語で状況を記述してください。"
)

# --- Build filter object ---
def build_filter(severities, sources):
    and_clauses = []
    if severities:
        or_clauses = [{"@eq": {"SEVERITY": s}} for s in severities]
        and_clauses.append({"@or": or_clauses})
    if sources:
        or_clauses = [{"@eq": {"SOURCE": s}} for s in sources]
        and_clauses.append({"@or": or_clauses})
    if and_clauses:
        return {"@and": and_clauses} if len(and_clauses) > 1 else and_clauses[0]
    return {}

# --- Execute Search ---
if search_clicked and search_query and search_query.strip():
    try:
        svc = (
            root
            .databases[DB]
            .schemas[SCHEMA]
            .cortex_search_services[SERVICE]
        )
        filter_obj = build_filter(sev_filter, src_filter)

        search_kwargs = {
            "query": search_query.strip(),
            "columns": ["LOG_ID", "TIMESTAMP", "SEVERITY", "SOURCE", "HOST", "MESSAGE"],
            "limit": max_results,
        }
        if filter_obj:
            search_kwargs["filter"] = filter_obj

        resp = svc.search(**search_kwargs)
        st.session_state["sem_results"] = resp.results
        st.session_state["sem_query"] = search_query.strip()

    except Exception as e:
        st.error(f"検索エラー: {e}")
        st.session_state["sem_results"] = None

# --- Display Results ---
if st.session_state.get("sem_results") is not None:
    results = st.session_state["sem_results"]
    st.subheader(f"検索結果: {len(results)} 件")

    if len(results) == 0:
        st.caption("該当するログが見つかりませんでした。別の表現で検索してみてください。")
    else:
        # Summary metrics
        sev_counts = {}
        for r in results:
            s = r.get("SEVERITY", "UNKNOWN")
            sev_counts[s] = sev_counts.get(s, 0) + 1

        cols = st.columns(min(len(sev_counts) + 1, 6))
        cols[0].metric("Total", len(results))
        for i, (sev, cnt) in enumerate(sorted(sev_counts.items())):
            if i + 1 < len(cols):
                sev_class = sev.lower()
                cols[i + 1].markdown(
                    f'<span class="sev-badge sev-{sev_class}">{sev}</span>',
                    unsafe_allow_html=True,
                )
                cols[i + 1].metric(sev, cnt)

        # Result table
        rows = []
        for r in results:
            data = dict(r)
            rows.append({
                "LOG_ID": data.get("LOG_ID", ""),
                "TIMESTAMP": str(data.get("TIMESTAMP", ""))[:19],
                "SEVERITY": data.get("SEVERITY", ""),
                "SOURCE": data.get("SOURCE", ""),
                "HOST": data.get("HOST", ""),
                "MESSAGE": data.get("MESSAGE", ""),
            })
        result_df = pd.DataFrame(rows)
        st.dataframe(result_df, use_container_width=True)

        # --- RAG: AI Analysis Button ---
        st.markdown("---")
        if st.button("AI分析（まとめ・考察を生成）"):
            with st.spinner("Cortex Complete で分析中..."):
                log_lines = []
                for r in results:
                    data = dict(r)
                    sev = data.get("SEVERITY", "")
                    ts = str(data.get("TIMESTAMP", ""))[:19]
                    source = data.get("SOURCE", "")
                    host = data.get("HOST", "")
                    message = data.get("MESSAGE", "")
                    log_lines.append(
                        f"[{sev}] {ts} | {source} | {host} | {message}"
                    )
                context = "\n".join(log_lines)

                saved_query = st.session_state.get("sem_query", "")
                prompt = f"""あなたはログ分析の専門家です。以下のログデータはセマンティック検索によって「{saved_query}」というクエリに関連すると判定されたログです。

以下の観点で分析結果を日本語で出力してください：

1. **概要**: 抽出されたログ全体の傾向を簡潔にまとめてください。
2. **根本原因の推定**: ログの内容から推測される問題の根本原因を分析してください。
3. **影響範囲**: 影響を受けているホスト、サービス、重要度の分布を整理してください。
4. **推奨アクション**: 問題を解決するための具体的な対応策を提案してください。
5. **注意点**: 見落としやすいポイントや追加調査が必要な項目があれば指摘してください。

--- ログデータ ---
{context}
--- ログデータ終了 ---"""

                try:
                    result_df = session.sql(
                        "SELECT SNOWFLAKE.CORTEX.COMPLETE('claude-3-5-sonnet', ?) AS RESPONSE",
                        params=[prompt],
                    ).to_pandas()
                    ai_response = result_df["RESPONSE"].iloc[0]

                    st.markdown(
                        f'<div class="ai-analysis">'
                        f'<h3>AI分析結果</h3>'
                        f'</div>',
                        unsafe_allow_html=True,
                    )
                    st.markdown(ai_response)

                except Exception as e:
                    st.error(f"AI分析エラー: {e}")

else:
    # Help section when no query
    st.markdown("---")
    st.subheader("Semantic Search について")
    st.markdown("""
このページでは **Cortex Search Service** を使った **セマンティック検索** が行えます。

**キーワード検索との違い:**

| | キーワード検索 (SEARCH関数) | セマンティック検索 (Cortex Search) |
|---|---|---|
| **検索方式** | トークン一致（完全一致ベース） | AI埋め込みベクトルによる意味検索 |
| **入力** | キーワード（例: `timeout error`） | 自然言語（例: `接続が切れてサービスが応答しない`） |
| **強み** | 高速・正確なキーワードマッチ | 表現が異なっても意味が近いログを発見 |
| **ユースケース** | 既知のエラーメッセージを検索 | 未知の問題の類似事象を調査 |

**AI分析（RAG）機能:**

検索結果が表示された後、「AI分析（まとめ・考察を生成）」ボタンを押すと、
セマンティック検索で抽出されたログを **Cortex Complete (claude-3-5-sonnet)** に入力し、
以下の観点で自動分析を行います：

- 概要（ログ全体の傾向）
- 根本原因の推定
- 影響範囲の整理
- 推奨アクション
- 注意点・追加調査事項

**使い方:**
1. 上部の検索バーに、探したいログの状況を自然言語で入力
2. サイドバーで Severity / Source のフィルタを追加（任意）
3. 結果はAIが意味的に関連度が高いと判断した順に表示されます
4. 「AI分析」ボタンで抽出結果の自動まとめ・考察を生成

**技術情報:**
- **Cortex Search Service**: `LOG_SEARCH_APP.PUBLIC.LOG_SEMANTIC_SEARCH`
- **埋め込みモデル**: `snowflake-arctic-embed-l-v2.0`（多言語対応・日本語対応）
- **AI分析モデル**: `claude-3-5-sonnet` (Cortex Complete)
- **検索対象カラム**: `MESSAGE`
- **フィルタ可能カラム**: `SEVERITY`, `SOURCE`, `HOST`
""")
