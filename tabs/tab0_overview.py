import numpy as np
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
import streamlit as st
from sqlalchemy import create_engine

from tabs.context import get_context

TAB_CODE = """

st.markdown('''
<style>
/* KPI Cards */
.kpi-card {
    background: #ffffff;
    border-radius: 12px;
    padding: 28px;
    text-align: center;
    box-shadow: 0 2px 12px rgba(0, 0, 0, 0.08);
    border: 1px solid #e8ecf1;
    position: relative;
    overflow: hidden;
    transition: box-shadow 0.2s ease, transform 0.2s ease;
}

.kpi-card:hover {
    box-shadow: 0 8px 24px rgba(0, 0, 0, 0.12);
    transform: translateY(-2px);
}

.kpi-card::before {
    content: '';
    position: absolute;
    top: 0;
    left: 0;
    right: 0;
    height: 4px;
}

.kpi-card.success::before { background: linear-gradient(90deg, #10b981, #34d399); }
.kpi-card.warning::before { background: linear-gradient(90deg, #f59e0b, #fbbf24); }
.kpi-card.danger::before { background: linear-gradient(90deg, #ef4444, #f87171); }
.kpi-card.info::before { background: linear-gradient(90deg, #3b82f6, #60a5fa); }

.kpi-value {
    font-size: 3em;
    font-weight: 700;
    margin: 0;
    line-height: 1.1;
}

.kpi-card.success .kpi-value { color: #10b981; }
.kpi-card.warning .kpi-value { color: #f59e0b; }
.kpi-card.danger .kpi-value { color: #ef4444; }
.kpi-card.info .kpi-value { color: #3b82f6; }

.kpi-label {
    color: #64748b;
    font-size: 0.9em;
    margin-top: 8px;
    text-transform: uppercase;
    letter-spacing: 0.5px;
    font-weight: 600;
}

.kpi-sublabel {
    color: #94a3b8;
    font-size: 0.85em;
    margin-top: 4px;
}

/* Section Headers */
.section-header {
    color: #1e293b;
    font-size: 1.25em;
    font-weight: 700;
    margin: 28px 0 16px 0;
    padding-bottom: 10px;
    border-bottom: 2px solid #e2e8f0;
    display: flex;
    align-items: center;
    gap: 10px;
}

/* Watch Card (Sites à surveiller) */
.watch-card {
    background: linear-gradient(135deg, #eff6ff 0%, #dbeafe 100%);
    border-radius: 10px;
    padding: 18px 22px;
    margin: 16px 0;
    border: 1px solid #bfdbfe;
}

.watch-card .title {
    color: #1d4ed8;
    font-weight: 600;
    font-size: 1.05em;
    display: flex;
    align-items: center;
    gap: 8px;
}

.watch-card .sites {
    color: #64748b;
    font-size: 0.9em;
    margin-top: 6px;
}

/* Equipment Header */
.equipment-header {
    color: #0369a1;
    font-weight: 600;
    font-size: 0.95em;
    margin: 14px 0 10px 0;
    padding: 6px 14px;
    background: #f0f9ff;
    border-radius: 6px;
    display: inline-block;
    border: 1px solid #bae6fd;
}

/* Defect Card */
.defect-card {
    background: #ffffff;
    border-radius: 10px;
    padding: 16px 18px;
    margin-bottom: 10px;
    border-left: 4px solid;
    box-shadow: 0 1px 6px rgba(0, 0, 0, 0.06);
    transition: transform 0.15s ease, box-shadow 0.15s ease;
}

.defect-card:hover {
    transform: translateX(3px);
    box-shadow: 0 4px 12px rgba(0, 0, 0, 0.1);
}

.defect-card.critical { 
    border-left-color: #ef4444;
    background: linear-gradient(90deg, #fef2f2 0%, #ffffff 20%);
}

.defect-card.warning { 
    border-left-color: #f59e0b;
    background: linear-gradient(90deg, #fffbeb 0%, #ffffff 20%);
}

.defect-card .defect-name {
    color: #1e293b;
    font-weight: 600;
    font-size: 0.95em;
    margin-bottom: 6px;
}

.defect-card .equipment {
    color: #64748b;
    font-size: 0.85em;
}

.defect-card .duration {
    color: #94a3b8;
    font-size: 0.8em;
    margin-top: 8px;
    font-style: italic;
}

/* Divider */
.dash-divider {
    height: 1px;
    background: linear-gradient(90deg, transparent, #e2e8f0, transparent);
    margin: 28px 0;
}

/* Empty State */
.empty-state {
    text-align: center;
    padding: 32px;
    color: #94a3b8;
    background: #f8fafc;
    border-radius: 10px;
}

.empty-state .icon {
    font-size: 2.5em;
    margin-bottom: 12px;
}

.empty-state .message {
    font-size: 1em;
    color: #64748b;
}
</style>
''', unsafe_allow_html=True)

df_alertes = pd.DataFrame()
df_defauts_actifs = pd.DataFrame()

try:
    CONFIG_KPI = {
        "host": "162.19.251.55",
        "port": 3306,
        "user": "nidec",
        "password": "MaV38f5xsGQp83",
        "database": "Charges",
    }

    engine = create_engine(
        "mysql+pymysql://{user}:{password}@{host}:{port}/{database}".format(**CONFIG_KPI)
    )

    query_alertes = \"\"\"
        SELECT
            Site,
            PDC,
            type_erreur,
            detection,
            occurrences_12h,
            moment,
            evi_code,
            downstream_code_pc
        FROM kpi_alertes
        ORDER BY detection DESC
    \"\"\"

    df_alertes = pd.read_sql(query_alertes, con=engine)

    query_defauts_actifs = \"\"\"
        SELECT
            site,
            date_debut,
            defaut,
            eqp
        FROM kpi_defauts_log
        WHERE date_fin IS NULL
        ORDER BY date_debut DESC
    \"\"\"

    df_defauts_actifs = pd.read_sql(query_defauts_actifs, con=engine)
    engine.dispose()

except Exception as e:
    st.error(f"Erreur de connexion: {str(e)}")

if not df_defauts_actifs.empty:
    df_defauts_actifs["date_debut"] = pd.to_datetime(df_defauts_actifs["date_debut"], errors="coerce")

    if site_sel:
        df_defauts_actifs = df_defauts_actifs[df_defauts_actifs["site"].isin(site_sel)]

nb_defauts_actifs = len(df_defauts_actifs) if not df_defauts_actifs.empty else 0
nb_sites_concernes = df_defauts_actifs["site"].nunique() if not df_defauts_actifs.empty else 0

if nb_defauts_actifs > 5:
    card_status = "danger"
elif nb_defauts_actifs > 0:
    card_status = "warning"
else:
    card_status = "success"

st.markdown(f'''
<div class="kpi-card {card_status}">
    <p class="kpi-value">{nb_defauts_actifs}</p>
    <p class="kpi-label">défaut{'s' if nb_defauts_actifs != 1 else ''} en cours</p>
    <p class="kpi-sublabel">sur {nb_sites_concernes} site{'s' if nb_sites_concernes != 1 else ''}</p>
</div>
''', unsafe_allow_html=True)

filter_pdc_only = st.checkbox("PDC uniquement", value=False, key="defauts_actifs_pdc_filter")

if not df_defauts_actifs.empty:
    if filter_pdc_only:
        df_defauts_actifs = df_defauts_actifs[df_defauts_actifs["eqp"].str.contains("PDC", case=False, na=False)]

if nb_defauts_actifs > 0 and not df_defauts_actifs.empty:
    now = pd.Timestamp.now()
    delta = now - df_defauts_actifs["date_debut"]
    df_defauts_actifs["Depuis (jours)"] = delta.dt.days
    df_defauts_actifs["is_recent"] = delta < pd.Timedelta(days=1)

    sites_recent = df_defauts_actifs.groupby("site")["is_recent"].any()
    nb_sites_recent = int(sites_recent.sum())

    if nb_sites_recent > 0:
        suffix = "s" if nb_sites_recent > 1 else ""
        sites_to_watch = list(sites_recent[sites_recent].index)
        sites_str = ", ".join(sites_to_watch)

        st.markdown(f'''
<div class="watch-card">
    <div class="title">🔍 <strong>{nb_sites_recent}</strong> site{suffix} à surveiller (défauts &lt; 24h)</div>
    <div class="sites">Sites : {sites_str}</div>
</div>
''', unsafe_allow_html=True)

    sites_groupes = df_defauts_actifs.groupby("site")

    for site_name, df_site in sites_groupes:
        nb_defauts_site = len(df_site)

        with st.expander(f"📍 {site_name} ({nb_defauts_site} défaut{'s' if nb_defauts_site > 1 else ''})", expanded=False):
            num_cols = 3
            equip_patterns = [
                ("PDC1", r"PDC1"),
                ("PDC2", r"PDC2"),
                ("PDC3", r"PDC3"),
                ("PDC4", r"PDC4"),
                ("Variateur HC1", r"Variateur.*HC1|HC1.*Variateur"),
                ("Variateur HC2", r"Variateur.*HC2|HC2.*Variateur"),
                ("Variateur HB1", r"Variateur.*HB1|HB1.*Variateur"),
                ("Variateur HB2", r"Variateur.*HB2|HB2.*Variateur"),
            ]

            handled_mask = pd.Series(False, index=df_site.index)

            for label, pattern in equip_patterns:
                mask = df_site["eqp"].str.contains(pattern, case=False, na=False, regex=True)
                df_eqp = df_site[mask].sort_values("date_debut")

                if df_eqp.empty:
                    continue

                handled_mask |= mask

                st.markdown(f'<div class="equipment-header">🔧 {label}</div>', unsafe_allow_html=True)

                for i in range(0, len(df_eqp), num_cols):
                    cols = st.columns(num_cols)
                    for j, col in enumerate(cols):
                        idx = i + j
                        if idx < len(df_eqp):
                            row = df_eqp.iloc[idx]
                            with col:
                                card_class = "critical" if row["Depuis (jours)"] > 7 else "warning"
                                st.markdown(f'''
<div class="defect-card {card_class}">
    <div class="defect-name">⚠️ {row["defaut"]}</div>
    <div class="equipment">🔧 {row["eqp"]}</div>
    <div class="duration">Depuis {row["Depuis (jours)"]} jours</div>
</div>
''', unsafe_allow_html=True)

st.markdown('<div class="dash-divider"></div>', unsafe_allow_html=True)

col_kpi1, col_kpi2 = st.columns(2)

with col_kpi1:
    suspicious = tables.get("suspicious_under_1kwh", pd.DataFrame())
    nb_suspicious = 0
    if not suspicious.empty:
        df_s_temp = suspicious.copy()
        if "Datetime start" in df_s_temp.columns:
            ds = pd.to_datetime(df_s_temp["Datetime start"], errors="coerce")
            mask = ds.ge(pd.Timestamp(d1)) & ds.lt(pd.Timestamp(d2) + pd.Timedelta(days=1))
            df_s_temp = df_s_temp[mask]
        if site_sel and "Site" in df_s_temp.columns:
            df_s_temp = df_s_temp[df_s_temp["Site"].isin(site_sel)]
        nb_suspicious = len(df_s_temp)

    if nb_suspicious > 5:
        status = "danger"
    elif nb_suspicious > 0:
        status = "warning"
    else:
        status = "success"

    st.markdown(f'''
<div class="section-header">Transactions Suspectes</div>
<div class="kpi-card {status}">
    <p class="kpi-value">{nb_suspicious}</p>
    <p class="kpi-label">Transactions &lt;1 kWh</p>
</div>
''', unsafe_allow_html=True)

with col_kpi2:
    multi_attempts = tables.get("multi_attempts_hour", pd.DataFrame())
    nb_multi_attempts = 0
    if not multi_attempts.empty:
        dfm_temp = multi_attempts.copy()
        if "Date_heure" in dfm_temp.columns:
            dfm_temp["Date_heure"] = pd.to_datetime(dfm_temp["Date_heure"], errors="coerce")
            d1_ts = pd.Timestamp(d1)
            d2_ts = pd.Timestamp(d2) + pd.Timedelta(days=1)
            mask = dfm_temp["Date_heure"].between(d1_ts, d2_ts)
            dfm_temp = dfm_temp[mask]
        if site_sel and "Site" in dfm_temp.columns:
            dfm_temp = dfm_temp[dfm_temp["Site"].isin(site_sel)]
        nb_multi_attempts = len(dfm_temp)

    if nb_multi_attempts > 5:
        status = "danger"
    elif nb_multi_attempts > 0:
        status = "warning"
    else:
        status = "success"

    st.markdown(f'''
<div class="section-header">Tentatives Multiples</div>
<div class="kpi-card {status}">
    <p class="kpi-value">{nb_multi_attempts}</p>
    <p class="kpi-label">Utilisateurs multi-tentatives</p>
</div>
''', unsafe_allow_html=True)

st.markdown('<div class="dash-divider"></div>', unsafe_allow_html=True)


if not df_alertes.empty:
    df_alertes["detection"] = pd.to_datetime(df_alertes["detection"], errors="coerce")

    start_dt = pd.to_datetime(d1)
    end_dt = pd.to_datetime(d2) + pd.Timedelta(days=1)
    df_alertes = df_alertes[df_alertes["detection"].between(start_dt, end_dt)]

    if site_sel:
        df_alertes = df_alertes[df_alertes["Site"].isin(site_sel)]

nb_alertes_actives = len(df_alertes) if not df_alertes.empty else 0

col_alert1, col_alert2 = st.columns(2)

with col_alert1:
    if nb_alertes_actives > 10:
        status = "danger"
    elif nb_alertes_actives > 0:
        status = "warning"
    else:
        status = "success"

    st.markdown(f'''
<div class="section-header">Alertes Actives</div>
<div class="kpi-card {status}">
    <p class="kpi-value">{nb_alertes_actives}</p>
    <p class="kpi-label">Alertes détectées</p>
</div>
''', unsafe_allow_html=True)

with col_alert2:
    st.markdown('<div class="section-header">Top 5 Sites en Alerte</div>', unsafe_allow_html=True)
    
    if not df_alertes.empty:
        top_sites_alertes = (
            df_alertes.groupby("Site")
            .size()
            .sort_values(ascending=False)
            .head(5)
        )

        fig_sites = go.Figure(go.Bar(
            x=top_sites_alertes.values,
            y=top_sites_alertes.index,
            orientation='h',
            marker=dict(
                color=top_sites_alertes.values,
                colorscale=[[0, '#fca5a5'], [0.5, '#ef4444'], [1, '#b91c1c']],
                showscale=False
            ),
            text=top_sites_alertes.values,
            textposition='outside',
            textfont=dict(color='#374151', size=12)
        ))

        fig_sites.update_layout(
            height=300,
            margin=dict(l=0, r=40, t=10, b=0),
            xaxis_title="",
            yaxis_title="",
            showlegend=False,
            paper_bgcolor='rgba(0,0,0,0)',
            plot_bgcolor='rgba(0,0,0,0)',
            font=dict(color='#475569'),
            xaxis=dict(
                showgrid=True,
                gridcolor='#f1f5f9',
                zeroline=False
            ),
            yaxis=dict(
                showgrid=False,
                zeroline=False
            )
        )

        st.plotly_chart(fig_sites, use_container_width=True)
    else:
        st.markdown('''
<div class="empty-state">
    <div class="icon">✅</div>
    <div class="message">Aucun site en alerte</div>
</div>
''', unsafe_allow_html=True)

st.markdown('<div class="dash-divider"></div>', unsafe_allow_html=True)

stat_global = (
    sess_kpi.groupby(SITE_COL)
    .agg(
        Total=("is_ok", "count"),
        Total_OK=("is_ok", "sum"),
    )
    .reset_index()
)
stat_global["Total_NOK"] = stat_global["Total"] - stat_global["Total_OK"]
stat_global["% OK"] = (
    np.where(stat_global["Total"].gt(0), stat_global["Total_OK"] / stat_global["Total"] * 100, 0)
).round(1)

if not stat_global.empty:
    top_charges = stat_global.sort_values("Total", ascending=False).head(10)
    by_site_success = top_charges.sort_values("% OK", ascending=False)
    by_site_fails = stat_global.sort_values("Total_NOK", ascending=False).head(10)

    col_chart1, col_chart2 = st.columns(2)

    with col_chart1:
        st.markdown('<div class="section-header"> Top 10 Sites avec le plus de charges - Taux de Réussite</div>', unsafe_allow_html=True)
        
        fig_success = go.Figure(go.Bar(
            x=by_site_success["% OK"],
            y=by_site_success[SITE_COL],
            orientation='h',
            marker=dict(
                color=by_site_success["% OK"],
                colorscale=[[0, '#fca5a5'], [0.5, '#fbbf24'], [1, '#34d399']],
                showscale=False,
                cmin=0,
                cmax=100
            ),
            text=by_site_success["% OK"].apply(lambda x: f"{x:.1f}%"),
            textposition='outside',
            textfont=dict(color='#374151', size=11)
        ))

        fig_success.update_layout(
            height=400,
            margin=dict(l=0, r=50, t=10, b=0),
            xaxis_title="",
            yaxis_title="",
            xaxis=dict(
                range=[0, 110],
                showgrid=True,
                gridcolor='#f1f5f9',
                zeroline=False
            ),
            yaxis=dict(showgrid=False, zeroline=False),
            paper_bgcolor='rgba(0,0,0,0)',
            plot_bgcolor='rgba(0,0,0,0)',
            font=dict(color='#475569')
        )

        st.plotly_chart(fig_success, use_container_width=True)

    with col_chart2:
        st.markdown('<div class="section-header"> Top 10 Sites - Nombre d\\'Échecs</div>', unsafe_allow_html=True)
        
        fig_fails = go.Figure(go.Bar(
            x=by_site_fails["Total_NOK"],
            y=by_site_fails[SITE_COL],
            orientation='h',
            marker=dict(
                color=by_site_fails["Total_NOK"],
                colorscale=[[0, '#fca5a5'], [0.5, '#ef4444'], [1, '#b91c1c']],
                showscale=False
            ),
            text=by_site_fails["Total_NOK"],
            textposition='outside',
            textfont=dict(color='#374151', size=11)
        ))

        fig_fails.update_layout(
            height=400,
            margin=dict(l=0, r=40, t=10, b=0),
            xaxis_title="",
            yaxis_title="",
            xaxis=dict(
                showgrid=True,
                gridcolor='#f1f5f9',
                zeroline=False
            ),
            yaxis=dict(showgrid=False, zeroline=False),
            paper_bgcolor='rgba(0,0,0,0)',
            plot_bgcolor='rgba(0,0,0,0)',
            font=dict(color='#475569')
        )

        st.plotly_chart(fig_fails, use_container_width=True)
"""

def render():
    ctx = get_context()
    globals_dict = {
        "np": np,
        "pd": pd,
        "px": px,
        "go": go,
        "st": st,
        "create_engine": create_engine
    }
    local_vars = dict(ctx.__dict__)
    local_vars.setdefault('plot', getattr(ctx, 'plot', None))
    local_vars.setdefault('hide_zero_labels', getattr(ctx, 'hide_zero_labels', None))
    local_vars.setdefault('with_charge_link', getattr(ctx, 'with_charge_link', None))
    local_vars.setdefault('evi_counts_pivot', getattr(ctx, 'evi_counts_pivot', None))
    local_vars = {k: v for k, v in local_vars.items() if v is not None}
    exec(TAB_CODE, globals_dict, local_vars)