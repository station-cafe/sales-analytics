#!/usr/bin/env python3
"""Stage 2: Compute all analytics from cached data.

Reads data/*.json, computes metrics, outputs analysis.json with
pre-computed Plotly figure JSON and summary statistics.

Usage:
    python3 analyze.py
"""

import json
from collections import Counter, defaultdict
from datetime import datetime, timedelta
from itertools import combinations
from pathlib import Path

import pandas as pd
import plotly.graph_objects as go
import plotly.express as px
from plotly.subplots import make_subplots

DATA_DIR = Path(__file__).parent / "data"
OUTPUT = Path(__file__).parent / "analysis.json"

STATION_MENU_ID = "7SZUNIBCQEUANKT7GB3R6JXW"
OPEN_DATE = "2026-02-22"

# Brand colors
CREAM = "#faf7f2"
ESPRESSO = "#2c1810"
TERRACOTTA = "#9b4a2c"
WARM_BROWN = "#6b3a2a"
SAGE = "#7a8b6f"
GOLD = "#c4953a"
SLATE = "#4a5568"

PALETTE = [TERRACOTTA, ESPRESSO, SAGE, GOLD, SLATE, WARM_BROWN, "#d4a574", "#8b6f5e"]

# WMO weather codes → descriptions
WMO_CODES = {
    0: "Clear", 1: "Mainly Clear", 2: "Partly Cloudy", 3: "Overcast",
    45: "Fog", 48: "Rime Fog",
    51: "Light Drizzle", 53: "Drizzle", 55: "Heavy Drizzle",
    61: "Light Rain", 63: "Rain", 65: "Heavy Rain",
    66: "Freezing Rain", 67: "Heavy Freezing Rain",
    71: "Light Snow", 73: "Snow", 75: "Heavy Snow", 77: "Snow Grains",
    80: "Light Showers", 81: "Showers", 82: "Heavy Showers",
    85: "Light Snow Showers", 86: "Heavy Snow Showers",
    95: "Thunderstorm", 96: "Thunderstorm w/ Hail", 99: "Heavy Thunderstorm",
}

# NJ Transit Tuxedo station approximate departure times (weekday AM)
NJ_TRANSIT_AM = ["05:48", "06:18", "06:48", "07:18", "07:48", "08:18"]


def load_data():
    with open(DATA_DIR / "orders.json") as f:
        orders = json.load(f)["orders"]
    with open(DATA_DIR / "payments.json") as f:
        payments = json.load(f)["payments"]
    with open(DATA_DIR / "catalog.json") as f:
        catalog = json.load(f)["objects"]
    with open(DATA_DIR / "weather.json") as f:
        weather = json.load(f)
    with open(DATA_DIR / "team_members.json") as f:
        team_members = json.load(f)["team_members"]
    with open(DATA_DIR / "shifts.json") as f:
        shifts = json.load(f)["shifts"]
    return orders, payments, catalog, weather, team_members, shifts


def build_catalog_map(catalog):
    """Build variation_id → {item_name, category, price_cents} lookup."""
    station_cat_ids = {}
    for c in catalog:
        if c["type"] == "CATEGORY":
            cd = c.get("category_data", {})
            if cd.get("parent_category", {}).get("id") == STATION_MENU_ID:
                station_cat_ids[c["id"]] = cd.get("name", "???")

    var_map = {}
    for obj in catalog:
        if obj["type"] != "ITEM":
            continue
        itd = obj.get("item_data", {})
        name = itd.get("name", "???")

        cat_ids = itd.get("categories", [])
        if cat_ids and isinstance(cat_ids[0], dict):
            cat_ids = [c.get("id") for c in cat_ids]

        category = "Other"
        for cid in cat_ids:
            if cid in station_cat_ids:
                category = station_cat_ids[cid]
                break

        for var in itd.get("variations", []):
            vd = var.get("item_variation_data", {})
            price = vd.get("price_money", {})
            var_map[var["id"]] = {
                "item_name": name,
                "variation_name": vd.get("name", "Regular"),
                "category": category,
                "price_cents": int(price.get("amount", 0)) if price else 0,
            }
    return var_map, station_cat_ids


def build_orders_df(orders, var_map):
    """Flatten orders into a DataFrame of line items with order metadata."""
    rows = []
    for o in orders:
        created = pd.Timestamp(o["created_at"]).tz_convert("America/New_York")
        order_total = o.get("total_money", {}).get("amount", 0) / 100
        order_tip = o.get("total_tip_money", {}).get("amount", 0) / 100
        order_tax = o.get("total_tax_money", {}).get("amount", 0) / 100
        n_items = len(o.get("line_items", []))

        for li in o.get("line_items", []):
            cid = li.get("catalog_object_id", "")
            info = var_map.get(cid, {})

            rows.append({
                "order_id": o["id"],
                "created_at": created,
                "date": created.date(),
                "hour": created.hour,
                "minute": created.minute,
                "weekday": created.day_name(),
                "weekday_num": created.dayofweek,
                "week_num": created.isocalendar()[1],
                "item_name": info.get("item_name", li.get("name", "Unknown")),
                "variation_name": info.get("variation_name", li.get("variation_name", "")),
                "category": info.get("category", "Other"),
                "quantity": int(li.get("quantity", 1)),
                "line_total": li.get("total_money", {}).get("amount", 0) / 100,
                "order_total": order_total,
                "order_tip": order_tip,
                "order_tax": order_tax,
                "order_item_count": n_items,
            })

    df = pd.DataFrame(rows)
    df["date"] = pd.to_datetime(df["date"])
    return df


def build_payments_df(payments):
    """Build payments DataFrame with tip info."""
    rows = []
    for p in payments:
        created = pd.Timestamp(p["created_at"]).tz_convert("America/New_York")
        rows.append({
            "payment_id": p["id"],
            "order_id": p.get("order_id", ""),
            "created_at": created,
            "date": created.date(),
            "hour": created.hour,
            "amount": p.get("amount_money", {}).get("amount", 0) / 100,
            "tip": p.get("tip_money", {}).get("amount", 0) / 100 if p.get("tip_money") else 0,
            "source_type": p.get("source_type", "UNKNOWN"),
            "team_member_id": p.get("team_member_id"),
        })
    df = pd.DataFrame(rows)
    df["date"] = pd.to_datetime(df["date"])
    return df


def build_weather_df(weather):
    """Build daily weather DataFrame."""
    daily = weather.get("daily", {})
    df = pd.DataFrame({
        "date": pd.to_datetime(daily["time"]),
        "temp_max": daily.get("temperature_2m_max", []),
        "temp_min": daily.get("temperature_2m_min", []),
        "temp_mean": daily.get("temperature_2m_mean", []),
        "precip": daily.get("precipitation_sum", []),
        "rain": daily.get("rain_sum", []),
        "snow": daily.get("snowfall_sum", []),
        "weathercode": daily.get("weathercode", []),
        "wind_max": daily.get("windspeed_10m_max", []),
    })
    df["weather_desc"] = df["weathercode"].map(lambda x: WMO_CODES.get(int(x) if pd.notna(x) else 0, "Unknown"))
    df["is_rainy"] = df["precip"] > 0.1
    df["is_nice"] = (df["temp_mean"] > 45) & (df["precip"] < 0.05) & (df["weathercode"] < 45)
    return df


def fig_to_json(fig):
    """Convert Plotly figure to JSON-serializable dict."""
    return json.loads(fig.to_json())


# ── Chart Builders ──────────────────────────────────────────────────────

def chart_daily_revenue(daily):
    fig = go.Figure()
    fig.add_trace(go.Bar(
        x=daily["date"], y=daily["revenue"],
        name="Daily Revenue",
        marker_color=TERRACOTTA,
        opacity=0.7,
    ))
    if len(daily) >= 7:
        fig.add_trace(go.Scatter(
            x=daily["date"], y=daily["revenue"].rolling(7, min_periods=3).mean(),
            name="7-Day Avg",
            line=dict(color=ESPRESSO, width=3),
        ))
    fig.update_layout(
        title=None,
        xaxis_title="Date",
        yaxis_title="Revenue ($)",
        yaxis_tickprefix="$",
        template="plotly_white",
        plot_bgcolor=CREAM,
        paper_bgcolor="rgba(0,0,0,0)",
        font=dict(family="DM Sans"),
        legend=dict(orientation="h", y=1.1),
        margin=dict(l=50, r=20, t=20, b=50),
    )
    return fig


def chart_dow_revenue(daily):
    dow_order = ["Monday", "Tuesday", "Wednesday", "Thursday", "Friday", "Saturday", "Sunday"]
    dow = daily.groupby("weekday").agg(
        avg_revenue=("revenue", "mean"),
        avg_orders=("order_count", "mean"),
        total_revenue=("revenue", "sum"),
    ).reindex(dow_order)

    fig = make_subplots(specs=[[{"secondary_y": True}]])
    fig.add_trace(go.Bar(
        x=dow.index, y=dow["avg_revenue"],
        name="Avg Revenue",
        marker_color=TERRACOTTA,
    ), secondary_y=False)
    fig.add_trace(go.Scatter(
        x=dow.index, y=dow["avg_orders"],
        name="Avg Orders",
        line=dict(color=GOLD, width=3),
        mode="lines+markers",
    ), secondary_y=True)
    fig.update_layout(
        template="plotly_white",
        plot_bgcolor=CREAM,
        paper_bgcolor="rgba(0,0,0,0)",
        font=dict(family="DM Sans"),
        legend=dict(orientation="h", y=1.1),
        margin=dict(l=50, r=50, t=20, b=50),
    )
    fig.update_yaxes(title_text="Avg Revenue ($)", tickprefix="$", secondary_y=False)
    fig.update_yaxes(title_text="Avg Orders", secondary_y=True)
    return fig, dow


def chart_hourly_heatmap(items_df):
    dow_order = ["Monday", "Tuesday", "Wednesday", "Thursday", "Friday", "Saturday", "Sunday"]
    # Group by weekday and hour — count unique orders
    hourly = items_df.groupby(["weekday", "hour"])["order_id"].nunique().reset_index()
    hourly.columns = ["weekday", "hour", "orders"]
    pivot = hourly.pivot(index="weekday", columns="hour", values="orders").reindex(dow_order).fillna(0)

    fig = go.Figure(go.Heatmap(
        z=pivot.values,
        x=[f"{h}:00" for h in pivot.columns],
        y=pivot.index,
        colorscale=[[0, CREAM], [0.5, GOLD], [1, TERRACOTTA]],
        text=pivot.values.astype(int),
        texttemplate="%{text}",
        textfont=dict(size=11),
        hovertemplate="Day: %{y}<br>Hour: %{x}<br>Orders: %{z}<extra></extra>",
    ))
    fig.update_layout(
        template="plotly_white",
        plot_bgcolor=CREAM,
        paper_bgcolor="rgba(0,0,0,0)",
        font=dict(family="DM Sans"),
        margin=dict(l=100, r=20, t=20, b=50),
        xaxis_title="Hour of Day",
    )
    return fig, pivot


def chart_wow_growth(daily):
    """Week-over-week same-day comparison."""
    daily = daily.copy()
    daily["week_start"] = daily["date"] - pd.to_timedelta(daily["date"].dt.dayofweek, unit="D")
    daily["week_label"] = daily["week_start"].dt.strftime("Week of %b %d")

    dow_order = ["Monday", "Tuesday", "Wednesday", "Thursday", "Friday", "Saturday", "Sunday"]
    weeks = sorted(daily["week_label"].unique())

    fig = go.Figure()
    colors = [TERRACOTTA, ESPRESSO, SAGE, GOLD, SLATE, WARM_BROWN, "#d4a574", "#8b6f5e"]
    for i, week in enumerate(weeks):
        wk = daily[daily["week_label"] == week]
        wk_dow = wk.set_index("weekday").reindex(dow_order)
        fig.add_trace(go.Scatter(
            x=dow_order,
            y=wk_dow["revenue"].values,
            name=week,
            line=dict(color=colors[i % len(colors)], width=2 if i < len(weeks) - 1 else 4),
            mode="lines+markers",
            opacity=0.5 if i < len(weeks) - 1 else 1,
        ))
    fig.update_layout(
        template="plotly_white",
        plot_bgcolor=CREAM,
        paper_bgcolor="rgba(0,0,0,0)",
        font=dict(family="DM Sans"),
        yaxis_title="Revenue ($)",
        yaxis_tickprefix="$",
        legend=dict(orientation="h", y=-0.15),
        margin=dict(l=50, r=20, t=20, b=80),
    )
    return fig


def chart_top_items(items_df, n=20):
    item_stats = items_df.groupby("item_name").agg(
        total_revenue=("line_total", "sum"),
        total_qty=("quantity", "sum"),
        order_count=("order_id", "nunique"),
    ).sort_values("total_revenue", ascending=True).tail(n)

    fig = go.Figure(go.Bar(
        x=item_stats["total_revenue"],
        y=item_stats.index,
        orientation="h",
        marker_color=TERRACOTTA,
        text=item_stats["total_revenue"].apply(lambda x: f"${x:,.0f}"),
        textposition="auto",
    ))
    fig.update_layout(
        template="plotly_white",
        plot_bgcolor=CREAM,
        paper_bgcolor="rgba(0,0,0,0)",
        font=dict(family="DM Sans"),
        xaxis_title="Total Revenue ($)",
        xaxis_tickprefix="$",
        margin=dict(l=180, r=20, t=20, b=50),
        height=max(400, n * 28),
    )
    return fig, item_stats


def chart_top_items_qty(items_df, n=20):
    item_stats = items_df.groupby("item_name").agg(
        total_qty=("quantity", "sum"),
    ).sort_values("total_qty", ascending=True).tail(n)

    fig = go.Figure(go.Bar(
        x=item_stats["total_qty"],
        y=item_stats.index,
        orientation="h",
        marker_color=SAGE,
        text=item_stats["total_qty"].astype(int),
        textposition="auto",
    ))
    fig.update_layout(
        template="plotly_white",
        plot_bgcolor=CREAM,
        paper_bgcolor="rgba(0,0,0,0)",
        font=dict(family="DM Sans"),
        xaxis_title="Units Sold",
        margin=dict(l=180, r=20, t=20, b=50),
        height=max(400, n * 28),
    )
    return fig


def chart_category_mix(items_df):
    cat_rev = items_df.groupby("category")["line_total"].sum().sort_values(ascending=False)
    # Filter out tiny categories
    cat_rev = cat_rev[cat_rev > 0]

    fig = go.Figure(go.Pie(
        labels=cat_rev.index,
        values=cat_rev.values,
        hole=0.45,
        marker=dict(colors=PALETTE[:len(cat_rev)]),
        textinfo="label+percent",
        textfont=dict(size=13),
        hovertemplate="%{label}: $%{value:,.0f} (%{percent})<extra></extra>",
    ))
    fig.update_layout(
        template="plotly_white",
        paper_bgcolor="rgba(0,0,0,0)",
        font=dict(family="DM Sans"),
        margin=dict(l=20, r=20, t=20, b=20),
        showlegend=False,
    )
    return fig, cat_rev


def chart_category_trend(items_df):
    cat_daily = items_df.groupby(["date", "category"])["line_total"].sum().reset_index()
    cat_daily_pivot = cat_daily.pivot(index="date", columns="category", values="line_total").fillna(0)
    # 7-day rolling
    cat_smooth = cat_daily_pivot.rolling(7, min_periods=3).mean()

    fig = go.Figure()
    for i, col in enumerate(cat_smooth.columns):
        fig.add_trace(go.Scatter(
            x=cat_smooth.index,
            y=cat_smooth[col],
            name=col,
            stackgroup="one",
            line=dict(color=PALETTE[i % len(PALETTE)]),
        ))
    fig.update_layout(
        template="plotly_white",
        plot_bgcolor=CREAM,
        paper_bgcolor="rgba(0,0,0,0)",
        font=dict(family="DM Sans"),
        yaxis_title="Revenue ($, 7-day avg)",
        yaxis_tickprefix="$",
        legend=dict(orientation="h", y=-0.15),
        margin=dict(l=50, r=20, t=20, b=80),
    )
    return fig


def chart_aov_trend(daily):
    fig = go.Figure()
    fig.add_trace(go.Scatter(
        x=daily["date"], y=daily["aov"],
        mode="markers",
        marker=dict(color=TERRACOTTA, size=6, opacity=0.5),
        name="Daily AOV",
    ))
    if len(daily) >= 7:
        fig.add_trace(go.Scatter(
            x=daily["date"], y=daily["aov"].rolling(7, min_periods=3).mean(),
            line=dict(color=ESPRESSO, width=3),
            name="7-Day Avg",
        ))
    fig.update_layout(
        template="plotly_white",
        plot_bgcolor=CREAM,
        paper_bgcolor="rgba(0,0,0,0)",
        font=dict(family="DM Sans"),
        yaxis_title="Avg Order Value ($)",
        yaxis_tickprefix="$",
        legend=dict(orientation="h", y=1.1),
        margin=dict(l=50, r=20, t=20, b=50),
    )
    return fig


def chart_weather_correlation(daily_weather):
    fig = make_subplots(rows=1, cols=2, subplot_titles=("Temperature vs Revenue", "Precipitation Impact"))

    fig.add_trace(go.Scatter(
        x=daily_weather["temp_mean"],
        y=daily_weather["revenue"],
        mode="markers",
        marker=dict(
            color=daily_weather["precip"],
            colorscale=[[0, TERRACOTTA], [1, SLATE]],
            size=10,
            opacity=0.7,
            colorbar=dict(title="Precip (in)", x=0.45),
        ),
        text=daily_weather["date"].dt.strftime("%b %d (%A)"),
        hovertemplate="%{text}<br>Temp: %{x:.0f}°F<br>Revenue: $%{y:,.0f}<extra></extra>",
    ), row=1, col=1)

    # Rain vs no rain box
    rainy = daily_weather[daily_weather["is_rainy"]]
    dry = daily_weather[~daily_weather["is_rainy"]]
    fig.add_trace(go.Box(y=dry["revenue"], name="Dry Days", marker_color=GOLD, boxmean=True), row=1, col=2)
    fig.add_trace(go.Box(y=rainy["revenue"], name="Rainy Days", marker_color=SLATE, boxmean=True), row=1, col=2)

    fig.update_layout(
        template="plotly_white",
        plot_bgcolor=CREAM,
        paper_bgcolor="rgba(0,0,0,0)",
        font=dict(family="DM Sans"),
        showlegend=False,
        margin=dict(l=50, r=20, t=40, b=50),
    )
    fig.update_xaxes(title_text="Avg Temp (°F)", row=1, col=1)
    fig.update_yaxes(title_text="Revenue ($)", tickprefix="$", row=1, col=1)
    fig.update_yaxes(title_text="Revenue ($)", tickprefix="$", row=1, col=2)
    return fig


def chart_tips(payments_df):
    card = payments_df[payments_df["source_type"] == "CARD"].copy()
    card["tip_pct"] = (card["tip"] / card["amount"] * 100).clip(0, 100)

    # Tips by hour
    tip_hourly = card.groupby("hour").agg(
        avg_tip=("tip", "mean"),
        avg_tip_pct=("tip_pct", "mean"),
        tip_rate=("tip", lambda x: (x > 0).mean() * 100),
    )

    fig = make_subplots(rows=1, cols=2, subplot_titles=("Avg Tip by Hour", "Tip Rate by Hour (%)"))

    fig.add_trace(go.Bar(
        x=tip_hourly.index.map(lambda h: f"{h}:00"),
        y=tip_hourly["avg_tip"],
        marker_color=GOLD,
    ), row=1, col=1)

    fig.add_trace(go.Bar(
        x=tip_hourly.index.map(lambda h: f"{h}:00"),
        y=tip_hourly["tip_rate"],
        marker_color=SAGE,
    ), row=1, col=2)

    fig.update_layout(
        template="plotly_white",
        plot_bgcolor=CREAM,
        paper_bgcolor="rgba(0,0,0,0)",
        font=dict(family="DM Sans"),
        showlegend=False,
        margin=dict(l=50, r=20, t=40, b=50),
    )
    fig.update_yaxes(title_text="Avg Tip ($)", tickprefix="$", row=1, col=1)
    fig.update_yaxes(title_text="Tip Rate (%)", ticksuffix="%", row=1, col=2)
    return fig


def chart_staffing_model(items_df):
    """Orders per hour by weekday — with staffing threshold lines."""
    dow_order = ["Monday", "Tuesday", "Wednesday", "Thursday", "Friday", "Saturday", "Sunday"]
    hourly = items_df.groupby(["weekday", "hour"])["order_id"].nunique().reset_index()
    hourly.columns = ["weekday", "hour", "orders"]

    # Count number of each weekday in the data
    day_counts = items_df.groupby("weekday")["date"].nunique()

    # Average orders per hour per weekday
    hourly = hourly.merge(day_counts.rename("n_days"), left_on="weekday", right_index=True)
    hourly["avg_orders"] = hourly["orders"] / hourly["n_days"]

    pivot = hourly.pivot(index="weekday", columns="hour", values="avg_orders").reindex(dow_order).fillna(0)

    # Annotate with staffing levels
    staffing = pivot.map(lambda x: "2+" if x >= 10 else ("1-2" if x >= 5 else "1"))

    fig = go.Figure(go.Heatmap(
        z=pivot.values,
        x=[f"{h}:00" for h in pivot.columns],
        y=pivot.index,
        colorscale=[[0, "#e8f5e9"], [0.3, GOLD], [0.6, TERRACOTTA], [1, ESPRESSO]],
        text=pivot.values.round(1),
        texttemplate="%{text}",
        textfont=dict(size=11),
        hovertemplate="Day: %{y}<br>Hour: %{x}<br>Avg Orders: %{z:.1f}<extra></extra>",
        colorbar=dict(title="Avg Orders/Hr"),
    ))
    fig.update_layout(
        template="plotly_white",
        plot_bgcolor=CREAM,
        paper_bgcolor="rgba(0,0,0,0)",
        font=dict(family="DM Sans"),
        margin=dict(l=100, r=20, t=20, b=50),
        xaxis_title="Hour of Day",
    )
    return fig, pivot, staffing


def chart_morning_rush(items_df):
    """5-minute bins during morning hours to detect commuter patterns."""
    morning = items_df[(items_df["hour"] >= 6) & (items_df["hour"] <= 10)].copy()
    morning["time_bin"] = morning["hour"] * 60 + morning["minute"]
    morning["time_bin"] = (morning["time_bin"] // 5) * 5  # 5-min bins

    bin_counts = morning.groupby("time_bin")["order_id"].nunique().reset_index()
    bin_counts["time_label"] = bin_counts["time_bin"].apply(lambda m: f"{m // 60}:{m % 60:02d}")

    fig = go.Figure(go.Bar(
        x=bin_counts["time_label"],
        y=bin_counts["order_id"],
        marker_color=TERRACOTTA,
    ))

    # Add NJ Transit departure markers as shapes (vline doesn't work with categorical x)
    for t in NJ_TRANSIT_AM:
        fig.add_annotation(
            x=t, y=1, yref="paper",
            text=f"🚂 {t}", showarrow=True, arrowhead=2,
            ax=0, ay=-30, font=dict(size=9, color=SLATE),
        )

    fig.update_layout(
        template="plotly_white",
        plot_bgcolor=CREAM,
        paper_bgcolor="rgba(0,0,0,0)",
        font=dict(family="DM Sans"),
        xaxis_title="Time",
        yaxis_title="Orders (5-min bins, total across all days)",
        margin=dict(l=50, r=20, t=20, b=50),
    )
    return fig


def chart_order_size_dist(items_df):
    order_sizes = items_df.groupby("order_id")["quantity"].sum()
    fig = go.Figure(go.Histogram(
        x=order_sizes,
        marker_color=TERRACOTTA,
        opacity=0.8,
        nbinsx=max(int(order_sizes.max()), 10),
    ))
    fig.update_layout(
        template="plotly_white",
        plot_bgcolor=CREAM,
        paper_bgcolor="rgba(0,0,0,0)",
        font=dict(family="DM Sans"),
        xaxis_title="Items per Order",
        yaxis_title="Number of Orders",
        margin=dict(l=50, r=20, t=20, b=50),
    )
    return fig


def compute_item_pairings(items_df, min_support=10):
    """Find items frequently purchased together."""
    order_items = items_df.groupby("order_id")["item_name"].apply(set)
    multi_item_orders = order_items[order_items.map(len) >= 2]

    pair_counts = Counter()
    for items in multi_item_orders:
        for pair in combinations(sorted(items), 2):
            pair_counts[pair] += 1

    top_pairs = [(p[0], p[1], count) for p, count in pair_counts.most_common(15) if count >= min_support]
    return top_pairs


def compute_attach_rate(items_df):
    """% of drink orders that also include food."""
    drink_cats = {"Just Coffee", "Not Coffee", "Signature Sips"}
    food_cats = {"Lite Bites", "Sweets"}

    order_cats = items_df.groupby("order_id")["category"].apply(set)
    has_drink = order_cats.map(lambda cats: bool(cats & drink_cats))
    has_food = order_cats.map(lambda cats: bool(cats & food_cats))

    drink_orders = has_drink.sum()
    drink_plus_food = (has_drink & has_food).sum()

    return {
        "drink_orders": int(drink_orders),
        "drink_plus_food": int(drink_plus_food),
        "attach_rate": round(drink_plus_food / drink_orders * 100, 1) if drink_orders > 0 else 0,
    }


def compute_weekly_growth(daily):
    """Compute week-over-week revenue growth."""
    weekly = daily.set_index("date").resample("W-SUN")["revenue"].sum().reset_index()
    weekly.columns = ["week_ending", "revenue"]
    weekly["growth_pct"] = weekly["revenue"].pct_change() * 100
    weekly["week_label"] = weekly["week_ending"].dt.strftime("Week ending %b %d")
    return weekly


def compute_what_if(daily, items_df, payments_df):
    """Pre-compute what-if scenario parameters."""
    # Extended hours: estimate revenue per hour in late afternoon
    afternoon = items_df[(items_df["hour"] >= 14) & (items_df["hour"] <= 16)]
    afternoon_rev_per_hour = afternoon.groupby(["date", "hour"])["line_total"].sum().mean()

    # Current operating hours estimate
    first_order_hour = items_df.groupby("date")["hour"].min().median()
    last_order_hour = items_df.groupby("date")["hour"].max().median()

    # Average tip rate
    card_payments = payments_df[payments_df["source_type"] == "CARD"]
    avg_tip_rate = (card_payments["tip"].sum() / card_payments["amount"].sum() * 100) if len(card_payments) > 0 else 0

    # Loyalty program: industry data suggests 15-25% lift
    avg_daily_revenue = daily["revenue"].mean()

    return {
        "afternoon_rev_per_hour": round(afternoon_rev_per_hour, 2),
        "first_order_hour": int(first_order_hour),
        "last_order_hour": int(last_order_hour),
        "avg_tip_rate": round(avg_tip_rate, 1),
        "avg_daily_revenue": round(avg_daily_revenue, 2),
        "loyalty_lift_low": round(avg_daily_revenue * 0.15, 2),
        "loyalty_lift_high": round(avg_daily_revenue * 0.25, 2),
    }


def compute_underperformers(items_df, n_days):
    """Items ordered less than once per day."""
    item_freq = items_df.groupby("item_name").agg(
        total_qty=("quantity", "sum"),
        total_revenue=("line_total", "sum"),
        order_count=("order_id", "nunique"),
    )
    item_freq["orders_per_day"] = item_freq["order_count"] / n_days
    underperformers = item_freq[item_freq["orders_per_day"] < 1].sort_values("total_revenue", ascending=False)
    # Only include items with at least some orders (exclude truly zero items)
    underperformers = underperformers[underperformers["order_count"] >= 1].head(15)
    return underperformers


# ── Staff Correlation Analysis ──────────────────────────────────────────

def build_staff_name_map(team_members):
    """Build team_member_id → first name map."""
    return {m["id"]: m.get("given_name", "Unknown") for m in team_members}


def build_shifts_df(shifts, staff_names):
    """Build shifts DataFrame with staff names."""
    rows = []
    for s in shifts:
        if s.get("status") != "CLOSED":
            continue  # Skip open/in-progress shifts
        tmid = s.get("team_member_id", s.get("employee_id", ""))
        start = pd.Timestamp(s["start_at"])
        end = pd.Timestamp(s.get("end_at", s["start_at"]))
        if start.tzinfo is None:
            continue
        start = start.tz_convert("America/New_York")
        end = end.tz_convert("America/New_York")
        hours = (end - start).total_seconds() / 3600
        if hours < 0.5:
            continue  # Skip very short shifts (clock errors)

        wage = s.get("wage", {})
        rows.append({
            "shift_id": s["id"],
            "team_member_id": tmid,
            "staff_name": staff_names.get(tmid, "Unknown"),
            "job_title": wage.get("title", ""),
            "start_at": start,
            "end_at": end,
            "date": start.normalize(),
            "hours": hours,
            "hourly_rate": wage.get("hourly_rate", {}).get("amount", 0) / 100,
            "declared_tips": s.get("declared_cash_tip_money", {}).get("amount", 0) / 100 if s.get("declared_cash_tip_money") else 0,
        })
    return pd.DataFrame(rows)


def compute_staff_correlation(shifts_df, items_df, staff_names):
    """Correlate staff on duty with sales volume — NOT attributional.

    For each shift, compute the order volume and revenue that occurred
    during the shift window. This shows correlation (what happens when
    this person is working) not causation (this person generated these sales).
    """
    if shifts_df.empty:
        return {}, None, []

    # For each day, determine who was working and what the sales were
    # Group shifts by date to see staffing levels
    daily_staff = shifts_df.groupby("date").agg(
        staff_count=("team_member_id", "nunique"),
        total_labor_hours=("hours", "sum"),
        staff_on_duty=("staff_name", lambda x: ", ".join(sorted(set(x)))),
    ).reset_index()

    # Daily revenue from items_df
    daily_revenue = items_df.groupby("date").agg(
        revenue=("order_total", lambda x: x.drop_duplicates().sum()),
        order_count=("order_id", "nunique"),
    ).reset_index()

    # Normalize dates to tz-naive for merging
    daily_staff["date"] = pd.to_datetime(daily_staff["date"]).dt.tz_localize(None)
    daily_revenue["date"] = pd.to_datetime(daily_revenue["date"]).dt.tz_localize(None)
    daily_merged = daily_staff.merge(daily_revenue, on="date", how="inner")

    # Per-staff metrics: for each staff member, what's the avg daily revenue
    # and order volume on days they work?
    staff_metrics = []
    for tmid, name in staff_names.items():
        days_worked = shifts_df[shifts_df["team_member_id"] == tmid]["date"].unique()
        if len(days_worked) == 0:
            continue

        days_data = daily_revenue[daily_revenue["date"].isin(days_worked)]
        if days_data.empty:
            continue

        total_hours = shifts_df[shifts_df["team_member_id"] == tmid]["hours"].sum()
        job_titles = shifts_df[shifts_df["team_member_id"] == tmid]["job_title"].value_counts().index.tolist()

        staff_metrics.append({
            "name": name,
            "team_member_id": tmid,
            "shifts_worked": len(days_worked),
            "total_hours": round(total_hours, 1),
            "avg_hours_per_shift": round(total_hours / len(days_worked), 1),
            "avg_daily_revenue_on_duty": round(days_data["revenue"].mean(), 2),
            "avg_daily_orders_on_duty": round(days_data["order_count"].mean(), 1),
            "job_title": job_titles[0] if job_titles else "",
        })

    staff_metrics.sort(key=lambda x: x["shifts_worked"], reverse=True)

    # Staffing level correlation: does more staff = more revenue?
    staffing_corr = {}
    if len(daily_merged) >= 5:
        corr = daily_merged[["staff_count", "total_labor_hours", "revenue", "order_count"]].corr()
        staffing_corr = {
            "staff_count_vs_revenue": round(corr.loc["staff_count", "revenue"], 3),
            "staff_count_vs_orders": round(corr.loc["staff_count", "order_count"], 3),
            "labor_hours_vs_revenue": round(corr.loc["total_labor_hours", "revenue"], 3),
        }

    # Revenue per labor hour
    if daily_merged["total_labor_hours"].sum() > 0:
        staffing_corr["revenue_per_labor_hour"] = round(
            daily_merged["revenue"].sum() / daily_merged["total_labor_hours"].sum(), 2
        )
        staffing_corr["avg_staff_per_day"] = round(daily_merged["staff_count"].mean(), 1)

    return staffing_corr, daily_merged, staff_metrics


def chart_staff_volume_correlation(daily_merged):
    """Chart: staff count vs revenue scatter + labor hours bar."""
    if daily_merged is None or daily_merged.empty:
        return None

    fig = make_subplots(rows=1, cols=2,
                        subplot_titles=("Staff Count vs Daily Revenue", "Revenue per Labor Hour"))

    fig.add_trace(go.Scatter(
        x=daily_merged["staff_count"],
        y=daily_merged["revenue"],
        mode="markers",
        marker=dict(color=TERRACOTTA, size=10, opacity=0.6),
        text=daily_merged["date"].dt.strftime("%b %d (%A)"),
        hovertemplate="%{text}<br>Staff: %{x}<br>Revenue: $%{y:,.0f}<extra></extra>",
    ), row=1, col=1)

    # Revenue per labor hour by date
    daily_merged = daily_merged.copy()
    daily_merged["rev_per_hour"] = daily_merged["revenue"] / daily_merged["total_labor_hours"]
    fig.add_trace(go.Bar(
        x=daily_merged["date"],
        y=daily_merged["rev_per_hour"],
        marker_color=SAGE,
        hovertemplate="%{x|%b %d}<br>$/labor hr: $%{y:.0f}<extra></extra>",
    ), row=1, col=2)

    fig.update_layout(
        template="plotly_white",
        plot_bgcolor=CREAM,
        paper_bgcolor="rgba(0,0,0,0)",
        font=dict(family="DM Sans"),
        showlegend=False,
        margin=dict(l=50, r=20, t=40, b=50),
    )
    fig.update_xaxes(title_text="Staff on Duty", row=1, col=1)
    fig.update_yaxes(title_text="Revenue ($)", tickprefix="$", row=1, col=1)
    fig.update_yaxes(title_text="$/Labor Hour", tickprefix="$", row=1, col=2)
    return fig


def chart_staff_shifts_timeline(shifts_df):
    """Gantt-style chart showing who works when."""
    if shifts_df.empty:
        return None

    # Get most recent 14 days of shifts for readability
    recent = shifts_df.sort_values("start_at", ascending=False)
    cutoff = recent["date"].iloc[0] - pd.Timedelta(days=14)
    recent = recent[recent["date"] >= cutoff].sort_values("start_at")

    fig = go.Figure()
    staff_list = sorted(recent["staff_name"].unique())
    colors = {name: PALETTE[i % len(PALETTE)] for i, name in enumerate(staff_list)}

    for _, row in recent.iterrows():
        fig.add_trace(go.Bar(
            x=[(row["end_at"] - row["start_at"]).total_seconds() / 3600],
            y=[row["date"].strftime("%b %d")],
            base=[row["start_at"].hour + row["start_at"].minute / 60],
            orientation="h",
            marker_color=colors[row["staff_name"]],
            name=row["staff_name"],
            showlegend=False,
            hovertemplate=f"{row['staff_name']}<br>{row['start_at'].strftime('%I:%M %p')} – {row['end_at'].strftime('%I:%M %p')}<br>{row['hours']:.1f} hrs<extra></extra>",
        ))

    # Add legend entries
    for name, color in colors.items():
        fig.add_trace(go.Bar(x=[0], y=[""], marker_color=color, name=name, showlegend=True))

    fig.update_layout(
        template="plotly_white",
        plot_bgcolor=CREAM,
        paper_bgcolor="rgba(0,0,0,0)",
        font=dict(family="DM Sans"),
        barmode="stack",
        xaxis_title="Hour of Day",
        xaxis=dict(range=[5, 20], dtick=2),
        margin=dict(l=80, r=20, t=20, b=50),
        legend=dict(orientation="h", y=-0.15),
        height=max(300, len(recent["date"].unique()) * 35 + 100),
    )
    return fig


# ── Main Analysis ───────────────────────────────────────────────────────

def main():
    print("Loading data...")
    orders, payments, catalog, weather, team_members, shifts = load_data()
    var_map, station_cats = build_catalog_map(catalog)

    print("Building DataFrames...")
    items_df = build_orders_df(orders, var_map)
    payments_df = build_payments_df(payments)
    weather_df = build_weather_df(weather)
    staff_names = build_staff_name_map(team_members)
    shifts_df = build_shifts_df(shifts, staff_names)

    n_days = (items_df["date"].max() - items_df["date"].min()).days + 1
    n_orders = items_df["order_id"].nunique()

    # Daily aggregation
    daily = items_df.groupby("date").agg(
        revenue=("order_total", lambda x: x.drop_duplicates().sum()),
        order_count=("order_id", "nunique"),
        item_count=("quantity", "sum"),
    ).reset_index()
    daily["aov"] = daily["revenue"] / daily["order_count"]
    daily["weekday"] = daily["date"].dt.day_name()
    daily["weekday_num"] = daily["date"].dt.dayofweek

    # ── Startup period segmentation ────────────────────────────────────
    # First 14 days = ramp-up; after that = steady state
    RAMP_UP_DAYS = 14
    open_date = pd.Timestamp(OPEN_DATE)
    steady_state_start = open_date + pd.Timedelta(days=RAMP_UP_DAYS)
    daily["period"] = daily["date"].apply(
        lambda d: "Ramp-Up (Weeks 1-2)" if d < steady_state_start else "Steady State"
    )
    items_df["period"] = items_df["date"].apply(
        lambda d: "Ramp-Up (Weeks 1-2)" if d < steady_state_start else "Steady State"
    )

    # Steady-state metrics (excludes ramp-up)
    daily_ss = daily[daily["period"] == "Steady State"]

    # Merge weather
    daily_weather = daily.merge(weather_df, on="date", how="left")

    # Flag severe weather days (heavy precip or snow)
    daily_weather["severe_weather"] = (
        (daily_weather["precip"].fillna(0) > 0.5) |
        (daily_weather["snow"].fillna(0) > 1) |
        (daily_weather["weathercode"].fillna(0) >= 65)
    )

    print(f"Analyzing {n_orders} orders over {n_days} days...")
    print(f"  Ramp-up: {len(daily) - len(daily_ss)} days | Steady state: {len(daily_ss)} days")
    print(f"  Severe weather days: {daily_weather['severe_weather'].sum()}")
    print(f"  Staff shifts: {len(shifts_df)}")

    # ── Build all charts ────────────────────────────────────────────────
    results = {"charts": {}, "stats": {}, "tables": {}}

    # Revenue
    results["charts"]["daily_revenue"] = fig_to_json(chart_daily_revenue(daily))
    dow_fig, dow_stats = chart_dow_revenue(daily)
    results["charts"]["dow_revenue"] = fig_to_json(dow_fig)
    results["charts"]["wow_growth"] = fig_to_json(chart_wow_growth(daily))
    results["charts"]["aov_trend"] = fig_to_json(chart_aov_trend(daily))

    # Time patterns
    heatmap_fig, heatmap_pivot = chart_hourly_heatmap(items_df)
    results["charts"]["hourly_heatmap"] = fig_to_json(heatmap_fig)
    results["charts"]["morning_rush"] = fig_to_json(chart_morning_rush(items_df))
    results["charts"]["order_size_dist"] = fig_to_json(chart_order_size_dist(items_df))

    # Menu
    top_rev_fig, top_rev_stats = chart_top_items(items_df)
    results["charts"]["top_items_revenue"] = fig_to_json(top_rev_fig)
    results["charts"]["top_items_qty"] = fig_to_json(chart_top_items_qty(items_df))
    cat_fig, cat_rev = chart_category_mix(items_df)
    results["charts"]["category_mix"] = fig_to_json(cat_fig)
    results["charts"]["category_trend"] = fig_to_json(chart_category_trend(items_df))

    # Weather
    results["charts"]["weather_correlation"] = fig_to_json(chart_weather_correlation(daily_weather))

    # Tips
    results["charts"]["tips"] = fig_to_json(chart_tips(payments_df))

    # Staffing
    staffing_fig, staffing_pivot, staffing_levels = chart_staffing_model(items_df)
    results["charts"]["staffing_model"] = fig_to_json(staffing_fig)

    # ── Summary stats ───────────────────────────────────────────────────
    total_revenue = daily["revenue"].sum()
    total_tips = payments_df["tip"].sum()
    weekly_growth = compute_weekly_growth(daily)

    results["stats"] = {
        "total_revenue": round(total_revenue, 2),
        "avg_daily_revenue": round(daily["revenue"].mean(), 2),
        "total_orders": n_orders,
        "avg_daily_orders": round(n_orders / n_days, 1),
        "avg_order_value": round(total_revenue / n_orders, 2),
        "total_items_sold": int(items_df["quantity"].sum()),
        "avg_items_per_order": round(items_df.groupby("order_id")["quantity"].sum().mean(), 1),
        "n_days": n_days,
        "date_range": f"{items_df['date'].min().strftime('%b %d')} – {items_df['date'].max().strftime('%b %d, %Y')}",
        "best_day_revenue": round(daily["revenue"].max(), 2),
        "best_day_date": daily.loc[daily["revenue"].idxmax(), "date"].strftime("%b %d (%A)"),
        "worst_day_revenue": round(daily["revenue"].min(), 2),
        "worst_day_date": daily.loc[daily["revenue"].idxmin(), "date"].strftime("%b %d (%A)"),
        "total_tips": round(total_tips, 2),
        "avg_tip_card": round(payments_df[payments_df["source_type"] == "CARD"]["tip"].mean(), 2),
        "tip_rate_pct": round(total_tips / total_revenue * 100, 1) if total_revenue > 0 else 0,
        "cash_pct": round(len(payments_df[payments_df["source_type"] == "CASH"]) / len(payments_df) * 100, 1),
        "card_pct": round(len(payments_df[payments_df["source_type"] == "CARD"]) / len(payments_df) * 100, 1),
        "peak_hour": int(items_df.groupby("hour")["order_id"].nunique().idxmax()),
        "busiest_day": dow_stats["avg_revenue"].idxmax(),
        "slowest_day": dow_stats["avg_revenue"].idxmin(),
        "top_item": top_rev_stats.index[-1],
        "top_item_revenue": round(top_rev_stats.iloc[-1]["total_revenue"], 2),
    }

    # Weekly growth
    if len(weekly_growth) >= 2:
        latest_growth = weekly_growth["growth_pct"].dropna()
        if len(latest_growth) > 0:
            results["stats"]["latest_weekly_growth"] = round(latest_growth.iloc[-1], 1)
            results["stats"]["avg_weekly_growth"] = round(latest_growth.mean(), 1)

    # ── Tables ──────────────────────────────────────────────────────────
    # Item pairings
    pairings = compute_item_pairings(items_df)
    results["tables"]["item_pairings"] = [
        {"item_a": a, "item_b": b, "count": c} for a, b, c in pairings
    ]

    # Attach rate
    results["stats"]["attach"] = compute_attach_rate(items_df)

    # Underperformers
    underperformers = compute_underperformers(items_df, n_days)
    results["tables"]["underperformers"] = [
        {
            "item": name,
            "orders_per_day": round(row["orders_per_day"], 2),
            "total_revenue": round(row["total_revenue"], 2),
            "total_orders": int(row["order_count"]),
        }
        for name, row in underperformers.iterrows()
    ]

    # Category breakdown
    results["tables"]["category_revenue"] = [
        {"category": cat, "revenue": round(rev, 2), "pct": round(rev / total_revenue * 100, 1)}
        for cat, rev in cat_rev.items()
    ]

    # Weekly summary
    results["tables"]["weekly_growth"] = [
        {
            "week": row["week_label"],
            "revenue": round(row["revenue"], 2),
            "growth_pct": round(row["growth_pct"], 1) if pd.notna(row["growth_pct"]) else None,
        }
        for _, row in weekly_growth.iterrows()
    ]

    # What-if parameters
    results["stats"]["what_if"] = compute_what_if(daily, items_df, payments_df)

    # Weather summary
    results["stats"]["weather"] = {
        "rainy_days": int(daily_weather["is_rainy"].sum()),
        "dry_days": int((~daily_weather["is_rainy"]).sum()),
        "avg_revenue_rainy": round(daily_weather[daily_weather["is_rainy"]]["revenue"].mean(), 2) if daily_weather["is_rainy"].any() else 0,
        "avg_revenue_dry": round(daily_weather[~daily_weather["is_rainy"]]["revenue"].mean(), 2),
        "rain_impact_pct": round(
            (1 - daily_weather[daily_weather["is_rainy"]]["revenue"].mean() / daily_weather[~daily_weather["is_rainy"]]["revenue"].mean()) * 100, 1
        ) if daily_weather["is_rainy"].any() else 0,
        "temp_correlation": round(daily_weather[["temp_mean", "revenue"]].corr().iloc[0, 1], 3) if len(daily_weather) > 5 else 0,
    }

    # Staffing recommendation table
    staffing_recs = []
    dow_order = ["Monday", "Tuesday", "Wednesday", "Thursday", "Friday", "Saturday", "Sunday"]
    for day in dow_order:
        if day in staffing_pivot.index:
            row = staffing_pivot.loc[day]
            peak = row.max()
            peak_hour = row.idxmax()
            level = "2+ staff" if peak >= 10 else ("1-2 staff" if peak >= 5 else "1 staff")
            staffing_recs.append({
                "day": day,
                "peak_orders_per_hour": round(peak, 1),
                "peak_hour": f"{peak_hour}:00",
                "recommendation": level,
            })
    results["tables"]["staffing_recs"] = staffing_recs

    # ── Staff correlation analysis ──────────────────────────────────────
    staffing_corr, daily_staff_merged, staff_metrics = compute_staff_correlation(
        shifts_df, items_df, staff_names
    )
    results["stats"]["staffing_corr"] = staffing_corr
    results["tables"]["staff_metrics"] = staff_metrics

    if daily_staff_merged is not None and not daily_staff_merged.empty:
        staff_corr_fig = chart_staff_volume_correlation(daily_staff_merged)
        if staff_corr_fig:
            results["charts"]["staff_volume_correlation"] = fig_to_json(staff_corr_fig)
    if not shifts_df.empty:
        timeline_fig = chart_staff_shifts_timeline(shifts_df)
        if timeline_fig:
            results["charts"]["staff_shifts_timeline"] = fig_to_json(timeline_fig)

    # ── Startup period stats ────────────────────────────────────────────
    rampup = daily[daily["period"] == "Ramp-Up (Weeks 1-2)"]
    ss = daily_ss
    severe = daily_weather[daily_weather["severe_weather"]]
    normal = daily_weather[~daily_weather["severe_weather"]]

    results["stats"]["periods"] = {
        "rampup_days": len(rampup),
        "rampup_avg_revenue": round(rampup["revenue"].mean(), 2) if len(rampup) > 0 else 0,
        "rampup_avg_orders": round(rampup["order_count"].mean(), 1) if len(rampup) > 0 else 0,
        "steady_days": len(ss),
        "steady_avg_revenue": round(ss["revenue"].mean(), 2) if len(ss) > 0 else 0,
        "steady_avg_orders": round(ss["order_count"].mean(), 1) if len(ss) > 0 else 0,
        "growth_from_rampup": round(
            (ss["revenue"].mean() / rampup["revenue"].mean() - 1) * 100, 1
        ) if len(rampup) > 0 and rampup["revenue"].mean() > 0 and len(ss) > 0 else 0,
        "severe_weather_days": int(daily_weather["severe_weather"].sum()),
        "severe_avg_revenue": round(severe["revenue"].mean(), 2) if len(severe) > 0 else 0,
        "normal_avg_revenue": round(normal["revenue"].mean(), 2) if len(normal) > 0 else 0,
        "severe_impact_pct": round(
            (1 - severe["revenue"].mean() / normal["revenue"].mean()) * 100, 1
        ) if len(severe) > 0 and len(normal) > 0 and normal["revenue"].mean() > 0 else 0,
    }

    # ── Save ────────────────────────────────────────────────────────────
    with open(OUTPUT, "w") as f:
        json.dump(results, f, default=str, indent=2)
    print(f"Saved analysis.json ({OUTPUT.stat().st_size:,} bytes)")
    print(f"\nKey metrics:")
    print(f"  Revenue: ${total_revenue:,.0f} over {n_days} days")
    print(f"  Avg daily: ${daily['revenue'].mean():,.0f} ({n_orders / n_days:.0f} orders/day)")
    print(f"  AOV: ${total_revenue / n_orders:.2f}")
    print(f"  Tips: ${total_tips:,.0f} ({total_tips / total_revenue * 100:.1f}% of revenue)")
    print(f"  Top item: {results['stats']['top_item']}")


if __name__ == "__main__":
    main()
