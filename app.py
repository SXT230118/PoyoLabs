from flask import Flask, jsonify, request, render_template, send_from_directory
from flask_cors import CORS
import requests # Make sure you have run 'pip install requests'
import statistics
import time
import os
import threading
import traceback
import json
# Optional dotenv support for local development secrets
try:
    # Import dynamically to avoid static analyzers/linting errors when
    # the 'python-dotenv' package is not installed in the environment.
    import importlib
    import importlib.util

    if importlib.util.find_spec('dotenv') is not None:
        load_dotenv = importlib.import_module('dotenv').load_dotenv
        _HAVE_DOTENV = True
    else:
        load_dotenv = None
        _HAVE_DOTENV = False
except Exception:
    load_dotenv = None
    _HAVE_DOTENV = False
from datetime import datetime, timedelta, timezone

# --- Networking safety defaults ---
DEFAULT_REQUEST_TIMEOUT = 5  # seconds for external API calls

if _HAVE_DOTENV:
    try:
        # Import find_dotenv dynamically to avoid static analyzers failing when
        # the 'python-dotenv' package is not installed in the environment.
        import importlib
        dotenv_mod = importlib.import_module('dotenv')
        find_dotenv = getattr(dotenv_mod, 'find_dotenv', None)
    except Exception:
        find_dotenv = None

    try:
        if callable(load_dotenv) and callable(find_dotenv):
            load_dotenv(find_dotenv())
        elif callable(load_dotenv):
            # fallback: call load_dotenv without an explicit path
            load_dotenv()
    except Exception:
        # ignore errors loading local env file
        pass

def safe_get(url, timeout=None, **kwargs):
    t = timeout or DEFAULT_REQUEST_TIMEOUT
    try:
        resp = requests.get(url, timeout=t, **kwargs)
        resp.raise_for_status()
        try:
            return resp.json()
        except Exception:
            return resp.text
    except Exception as e:
        print(f"[http] GET {url} failed: {e}")
        return None

def safe_post(url, json=None, timeout=None, **kwargs):
    t = timeout or DEFAULT_REQUEST_TIMEOUT
    try:
        resp = requests.post(url, json=json, timeout=t, **kwargs)
        resp.raise_for_status()
        try:
            return resp.json()
        except Exception:
            return resp.text
    except Exception as e:
        print(f"[http] POST {url} failed: {e}")
        return None

# Optional: NVIDIA Nemotron client (OpenAI-compatible wrapper)
try:
    from openai import OpenAI
    _HAS_NEMOTRON = True
except Exception:
    OpenAI = None
    _HAS_NEMOTRON = False

# --- Setup ---
app = Flask(__name__)
CORS(app) 

# Load local .env if python-dotenv is installed. This is optional and safe.
if _HAVE_DOTENV:
    try:
        # Use find_dotenv if available to locate the .env file in the repo tree
        dotenv_mod = importlib.import_module('dotenv')
        find_dotenv = getattr(dotenv_mod, 'find_dotenv', None)
        if callable(load_dotenv) and callable(find_dotenv):
            env_path = find_dotenv()
            if env_path:
                load_dotenv(env_path)
                # Don't print the key, just report presence
                if os.environ.get('NV_API_KEY') or os.environ.get('nv_api_key'):
                    print('[env] .env loaded and NV_API_KEY found (hidden)')
            else:
                # fallback to calling load_dotenv() which will try default filename
                load_dotenv()
                if os.environ.get('NV_API_KEY') or os.environ.get('nv_api_key'):
                    print('[env] .env loaded (default) and NV_API_KEY found (hidden)')
        elif callable(load_dotenv):
            # Fallback: call load_dotenv without explicit path
            load_dotenv()
            if os.environ.get('NV_API_KEY') or os.environ.get('nv_api_key'):
                print('[env] .env loaded (fallback) and NV_API_KEY found (hidden)')
    except Exception as e:
        # Don't crash the app for dotenv issues; just log them
        print('[env] dotenv load failed:', e)

# Expose a normalized environment lookup for NV API key. Accept either
# uppercase `NV_API_KEY` or lowercase `nv_api_key` (some users set envs in
# different case-sensitive shells). This helper avoids duplicating logic.
def _get_nv_api_key_from_env():
    return os.environ.get('NV_API_KEY') or os.environ.get('nv_api_key')
# ### NEW: Define the EOG API Base URL ###
EOG_API_BASE_URL = "https://hackutd2025.eog.systems" 

# --- Helper Function Definitions (Moved to top) ---

def _parse_timestamp(ts_str):
    """Parse ISO-like timestamps returned by the EOG API. Supports trailing Z."""
    if not ts_str:
        return None
    try:
        # Handle trailing Z
        if ts_str.endswith('Z'):
            ts_str = ts_str[:-1] + '+00:00'
        return datetime.fromisoformat(ts_str)
    except Exception:
        try:
            # fallback: try common format
            return datetime.strptime(ts_str, '%Y-%m-%dT%H:%M:%S')
        except Exception:
            return None

def _compute_rates_from_history(sample_limit=500):
    """Analyze the recent /api/Data time-series and compute per-cauldron
    median fill and drain rates (liters per minute).
    Returns a dict: { cauldron_id: {'fill_rate_per_min': x, 'drain_rate_per_min': y} }
    """
    try:
        # Request the full historic range per the challenge guidance so rate computation
        # uses minute-by-minute samples across the whole dataset when possible.
        raw = safe_get(EOG_API_BASE_URL + '/api/Data?start_date=0&end_date=2000000000', timeout=20) or {}
    except Exception:
        return {}

    # Normalize to list of records
    data_list = raw if isinstance(raw, list) else (raw.get('data') if isinstance(raw, dict) and isinstance(raw.get('data'), list) else [])
    # Keep only the last N samples
    if not data_list:
        return {}
    # If time-series shape (records with 'timestamp' and 'cauldron_levels')
    # then extract timestamps and per-cauldron values
    records = []
    for rec in data_list:
        if not isinstance(rec, dict):
            continue
        ts = None
        try:
            ts = rec.get('timestamp') or rec.get('time') or rec.get('t')
        except Exception:
            ts = None
        if not ts:
            continue
        parsed = None
        try:
            parsed = _parse_timestamp(ts)
        except Exception:
            parsed = None
        if not parsed:
            continue
        levels = rec.get('cauldron_levels') or rec.get('levels') or {}
        if not isinstance(levels, dict):
            continue
        records.append((parsed, levels))

    if not records:
        return {}

    # Use the last `sample_limit` records
    records = records[-sample_limit:]

    # Build per-cauldron time-ordered series
    per_series = {}
    for ts, levels in records:
        for cid, v in levels.items():
            try:
                num = float(v)
            except Exception:
                continue
            per_series.setdefault(cid, []).append((ts, num))

    rates = {}
    for cid, series in per_series.items():
        # compute per-interval rates
        fill_rates = []
        drain_rates = []
        for i in range(len(series)-1):
            t0, v0 = series[i]
            t1, v1 = series[i+1]
            dt_min = (t1 - t0).total_seconds() / 60.0
            if dt_min <= 0 or dt_min > 60*24:
                continue
            delta = v1 - v0
            rate = delta / dt_min
            if rate > 0:
                fill_rates.append(rate)
            elif rate < 0:
                drain_rates.append(abs(rate))

        # robust central tendency: median when possible, else mean, else 0
        def choose(vals):
            if not vals:
                return 0.0
            try:
                return float(statistics.median(vals))
            except Exception:
                try:
                    return float(sum(vals)/len(vals))
                except Exception:
                    return 0.0

        fill_r = choose(fill_rates)
        drain_r = choose(drain_rates)

        # Enforce reasonable bounds to avoid wild numbers
        if fill_r < 0 or fill_r > 1000:
            fill_r = 0.0
        if drain_r < 0 or drain_r > 5000:
            drain_r = 0.0

        rates[cid] = {
            'fill_rate_per_min': round(fill_r, 3),
            'drain_rate_per_min': round(drain_r, 3)
        }

    return rates

# --- NEW: Load ALL Static Data from the API on Startup ---
def load_static_factory_data():
    """
    Called ONCE when the server starts.
    Fetches all static data (names, network, etc.) from the API
    and stores it in memory.
    """
    print("Loading static factory data from EOG API...")
    try:
        # Use the endpoints from your screenshot
        cauldrons = safe_get(EOG_API_BASE_URL + "/api/Information/cauldrons") or []
        network = safe_get(EOG_API_BASE_URL + "/api/Information/network") or {}
        market = safe_get(EOG_API_BASE_URL + "/api/Information/market") or {}
        couriers = safe_get(EOG_API_BASE_URL + "/api/Information/couriers") or []
        
        # ### THE MOST IMPORTANT PART: FILL/DRAIN RATES ###
        # First try to ingest explicit metadata rates from the API.
        meta = None
        try:
            meta = safe_get(EOG_API_BASE_URL + '/api/Data/metadata', timeout=5)
        except Exception:
            meta = None

        meta_rates = {}
        if isinstance(meta, dict):
            for key in ('cauldron_rates', 'rates', 'fill_rates', 'per_cauldron'):
                if key in meta and isinstance(meta[key], dict):
                    meta_rates = meta[key]
                    break

        # If metadata didn't provide per-cauldron rates, compute them from history
        computed = {}
        try:
            computed = _compute_rates_from_history()
        except Exception:
            computed = {}

        for c in cauldrons:
            cid = c.get('id')
            rate_obj = None
            if cid and cid in meta_rates:
                rate_obj = meta_rates.get(cid)
            elif cid and cid in computed:
                rate_obj = computed.get(cid)

            if isinstance(rate_obj, dict):
                c['fill_rate_per_min'] = float(rate_obj.get('fill_rate_per_min', rate_obj.get('fill_rate', 0)))
                c['drain_rate_per_min'] = float(rate_obj.get('drain_rate_per_min', rate_obj.get('drain_rate', 0)))
            else:
                # Last-ditch: try a per-cauldron metadata query before falling back.
                per_rate = None
                try:
                    per_meta = safe_get(EOG_API_BASE_URL + f"/api/Data/metadata?cauldronId={cid}", timeout=5)
                    # metadata may contain nested maps like {'cauldron_rates': {cid: {...}}}
                    if isinstance(per_meta, dict):
                        for key in ('cauldron_rates', 'rates', 'fill_rates', 'per_cauldron'):
                            if key in per_meta and isinstance(per_meta[key], dict) and cid in per_meta[key]:
                                per_rate = per_meta[key].get(cid)
                                break
                        # or the response itself may directly contain rate fields
                        if per_rate is None and ('fill_rate_per_min' in per_meta or 'drain_rate_per_min' in per_meta):
                            per_rate = per_meta
                except Exception:
                    per_rate = None

                if isinstance(per_rate, dict):
                    c['fill_rate_per_min'] = float(per_rate.get('fill_rate_per_min', per_rate.get('fill_rate', 1.0)))
                    c['drain_rate_per_min'] = float(per_rate.get('drain_rate_per_min', per_rate.get('drain_rate', 12.0)))
                else:
                    # deterministic fallback (avoid randomness which confused debugging/UX)
                    fallback_fill = 1.0
                    fallback_drain = 12.0
                    c['fill_rate_per_min'] = fallback_fill
                    c['drain_rate_per_min'] = fallback_drain
                    print(f"[rates] No metadata/computed rates for cauldron {cid}; using fallback fill={fallback_fill}, drain={fallback_drain}")
        # --- Normalize coordinates for frontend ---
        # Accept many possible key names and nested structures, then coerce to floats
        def _find_coord(obj, *keys):
            # try dotted path first
            for k in keys:
                parts = k.split('.')
                cur = obj
                ok = True
                for p in parts:
                    if isinstance(cur, dict) and p in cur:
                        cur = cur[p]
                    else:
                        ok = False
                        break
                if ok and cur is not None:
                    return cur
            return None

        # possible names to look for
        lat_keys = ('lat','latitude','location.lat','coords.lat')
        lon_keys = ('lon','lng','longitude','location.lon','coords.lon')

        # normalize each cauldron
        for c in cauldrons:
            # try direct keys first
            lat = _find_coord(c, *lat_keys)
            lon = _find_coord(c, *lon_keys)
            # sometimes coordinates are strings inside nested objects
            try:
                latf = float(lat) if lat is not None and lat != '' else None
            except Exception:
                latf = None
            try:
                lonf = float(lon) if lon is not None and lon != '' else None
            except Exception:
                lonf = None

            if latf is not None and lonf is not None:
                c['lat'] = latf
                c['lon'] = lonf
            else:
                # leave missing for now; we'll fill deterministic positions below
                c['lat'] = c.get('lat') if isinstance(c.get('lat'), (int,float)) else None
                c['lon'] = c.get('lon') if isinstance(c.get('lon'), (int,float)) else None

        # If many cauldrons lack coords, generate deterministic grid positions centered on the market if available
        missing = [c for c in cauldrons if c.get('lat') is None or c.get('lon') is None]
        if missing:
            try:
                center_lat = float(market.get('latitude') or market.get('lat') or 0)
            except Exception:
                center_lat = 0.0
            try:
                center_lon = float(market.get('longitude') or market.get('lon') or 0)
            except Exception:
                center_lon = 0.0

            n = len(missing)
            cols = int(max(1, round(n**0.5)))
            rows = int((n + cols - 1) // cols)
            spacing = 0.02  # ~ small lat/lon delta to separate points (~2km depending on lat)
            idx = 0
            for r in range(rows):
                for cidx in range(cols):
                    if idx >= n:
                        break
                    node = missing[idx]
                    # place on a small grid around center
                    node['lat'] = center_lat + (r - rows/2) * spacing
                    node['lon'] = center_lon + (cidx - cols/2) * spacing
                    idx += 1

        print(f"Successfully loaded data for {len(cauldrons)} cauldrons.")
        
        return {
            "cauldrons": cauldrons,
            "network": network,
            "market": market,
            "couriers": couriers
        }
        
    except Exception as e:
        print(f"!!!!!!!!!!!!!! FAILED TO LOAD STATIC DATA !!!!!!!!!!!!!!")
        print(f"Error: {e}")
        print("Is the EOG API down or did the URL change?")
        print("!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!")
        return None

def _refresh_rates_periodically(interval_seconds=60):
    """Background thread: periodically recompute rates from history and update factory_static_data in-place.
    This ensures we use real data where available instead of relying on random fallbacks.
    """
    def loop():
        while True:
            try:
                computed = _compute_rates_from_history()
                if computed:
                    # update factory_static_data in place
                    for c in factory_static_data.get('cauldrons', []):
                        cid = c.get('id')
                        if cid in computed:
                            rates = computed[cid]
                            c['fill_rate_per_min'] = rates.get('fill_rate_per_min', c.get('fill_rate_per_min', 0))
                            c['drain_rate_per_min'] = rates.get('drain_rate_per_min', c.get('drain_rate_per_min', 0))
                    print('[rates] Updated fill/drain rates from history')
            except Exception as e:
                print(f'[rates] Background rate refresh failed: {e}')
            time.sleep(interval_seconds)

    t = threading.Thread(target=loop, daemon=True)
    t.start()


@app.route('/api/compute_rates')
def api_compute_rates():
    """Trigger an on-demand recompute of per-cauldron rates from history.
    Returns the computed rates (may be empty if history insufficient).
    """
    try:
        computed = _compute_rates_from_history()
        if computed:
            for c in factory_static_data.get('cauldrons', []):
                cid = c.get('id')
                if cid in computed:
                    rates = computed[cid]
                    c['fill_rate_per_min'] = rates.get('fill_rate_per_min', c.get('fill_rate_per_min', 0))
                    c['drain_rate_per_min'] = rates.get('drain_rate_per_min', c.get('drain_rate_per_min', 0))
        return jsonify({'computed': computed})
    except Exception as e:
        return jsonify({'error': str(e)}), 500


# This is now our global, in-memory map of the factory
factory_static_data = load_static_factory_data()
if factory_static_data is None:
    exit() # Stop the app if we can't load the map

# Server-side forecast smoothing state to avoid large upward jumps in full_at
forecast_state = {}

# Start background refresh of rates so we rely on real API-derived numbers where possible
try:
    _refresh_rates_periodically(interval_seconds=60)
except Exception:
    pass

# --- EOG Challenge: Tool Definitions (API Endpoints) ---

@app.route('/api/cauldron/levels')
def get_cauldron_levels():
    """
    Tool: Gets the current level of all cauldrons.
    This is called by the dashboard every 5 seconds.
    """
    
    # 1. Call the REAL EOG API for LIVE data
    try:
        # This endpoint is from your screenshot!
        live_data_url = EOG_API_BASE_URL + "/api/Data" 
        live_levels_data = safe_get(live_data_url)
        if live_levels_data is None:
            print(f"ERROR fetching from /api/Data: timeout or API error")
            return jsonify({"error": "Could not fetch /api/Data (timeout or API error)"}), 500
        
    except Exception as e:
        print(f"ERROR fetching from /api/Data: {e}")
        return jsonify({"error": str(e)}), 500

    # 2. MERGE live data with our static data
    merged_cauldron_data = []

    live_levels_list = live_levels_data
    if isinstance(live_levels_data, dict):
        # common wrappers
        for wrapper in ('data', 'items', 'results', 'value'):
            if wrapper in live_levels_data and isinstance(live_levels_data[wrapper], list):
                live_levels_list = live_levels_data[wrapper]
                break
        else:
            # single-object response
            if any(k in live_levels_data for k in ('cauldronId', 'id', 'cauldron_id', 'currentVolume', 'current_volume')):
                live_levels_list = [live_levels_data]
            else:
                app.logger.warning("Unexpected /api/Data JSON shape: %s", type(live_levels_data))
                try:
                    app.logger.debug("Payload: %s", live_levels_data)
                except Exception:
                    pass
                return jsonify({"error": "Unexpected /api/Data format"}), 500

    live_levels_map = {}
    if isinstance(live_levels_list, list) and live_levels_list:
        # detect time-series shape
        first = live_levels_list[0]
        if isinstance(first, dict) and 'cauldron_levels' in first and isinstance(first['cauldron_levels'], dict):
            # choose the latest record (assume list is chronological; pick last)
            latest = None
            for rec in reversed(live_levels_list):
                if isinstance(rec, dict) and isinstance(rec.get('cauldron_levels'), dict):
                    latest = rec['cauldron_levels']
                    break
            if latest is None:
                latest = {}
            # coerce values to floats where possible
            for k,v in latest.items():
                try:
                    live_levels_map[k] = float(v)
                except Exception:
                    live_levels_map[k] = v
        else:
            # fallback: treat as list of items with id/value fields
            for item in live_levels_list:
                if not isinstance(item, dict):
                    continue

                cauldron_key = None
                for k in ('cauldronId', 'cauldron_id', 'id'):
                    if k in item:
                        cauldron_key = item[k]
                        break

                if cauldron_key is None and isinstance(item.get('cauldron'), dict):
                    cauldron_key = item['cauldron'].get('id')

                if cauldron_key is None:
                    continue

                level = None
                for lvl_key in ('currentVolume', 'current_volume', 'volume', 'level', 'value', 'current'):
                    if lvl_key in item:
                        level = item[lvl_key]
                        break

                try:
                    if level is not None:
                        level = float(level)
                except Exception:
                    level = None

                live_levels_map[cauldron_key] = level
    elif isinstance(live_levels_list, dict):
        # single-object case already handled above; attempt to extract map
        if 'cauldron_levels' in live_levels_list and isinstance(live_levels_list['cauldron_levels'], dict):
            for k,v in live_levels_list['cauldron_levels'].items():
                try:
                    live_levels_map[k] = float(v)
                except Exception:
                    live_levels_map[k] = v

    for static_cauldron in factory_static_data['cauldrons']:
        cauldron_id = static_cauldron['id']
        merged_data = static_cauldron.copy()
        
        live_level = live_levels_map.get(cauldron_id)
        
        merged_data['current_level'] = live_level if live_level is not None else 0
        
        # Check for overflow
        if live_level and live_level >= merged_data['max_volume']:
            merged_data['anomaly'] = True
        else:
            merged_data['anomaly'] = False # Will be set by discrepancy check later

        merged_cauldron_data.append(merged_data)
        
    # 3. Return the fully merged data to our frontend
    return jsonify(merged_cauldron_data)


# *** BUG FIX: Allow live_levels_data to be passed in ***
@app.route('/api/logistics/forecast')
def forecast_fill_times(live_levels_data=None):
    """
    Tool (EOG Bonus): Forecasts fill times.
    Can accept live_levels_data to prevent a second API call.
    """
    
    forecasts = []
    
    # 1. Get live levels IF NOT provided
    if live_levels_data is None:
        try:
            live_levels_response = get_cauldron_levels()
            if live_levels_response.status_code != 200:
                return jsonify({"error": "Could not get live levels for forecast."})
            live_levels_data = live_levels_response.get_json() 
        except Exception as e:
            return jsonify({"error": str(e)})

    # 2. Loop through the live data and use static data to forecast
    for cauldron in live_levels_data:
        fill_rate = cauldron.get('fill_rate_per_min', 0)
        
        if cauldron['current_level'] < cauldron['max_volume']:
            liters_to_full = cauldron['max_volume'] - cauldron['current_level']
            
            if fill_rate > 0:
                time_to_full_min = liters_to_full / fill_rate
                forecasts.append({
                    "cauldron_id": cauldron['id'],
                    "name": cauldron['name'],
                    "time_to_full_min": round(time_to_full_min, 1)
                })
    
    # This function is called by another python function,
    # so return the raw list, not a Flask Response
    if live_levels_data is not None:
        return forecasts
    
    return jsonify(forecasts)


@app.route('/api/cauldron/status')
def cauldron_status():
    """
    Returns merged cauldron data including current level, percentage full,
    and estimated time to full (minutes) by calling existing tools.
    Frontend dashboard will poll this endpoint.
    """
    try:
        live_levels_response = get_cauldron_levels()
        if live_levels_response.status_code != 200:
            return live_levels_response
        live_levels = live_levels_response.get_json()
    except Exception as e:
        return jsonify({"error": f"Could not fetch live levels: {e}"}), 500

    try:
        # *** BUG FIX: Pass the live_levels data to stop the race condition ***
        forecasts = forecast_fill_times(live_levels_data=live_levels)
        
    except Exception as e:
        print(f"Error in forecast_fill_times: {e}")
        forecasts = []

    # Build a lookup of forecast by cauldron_id
    forecast_map = {f.get('cauldron_id'): f for f in (forecasts or [])}

    status_list = []
    for c in live_levels:
        max_vol = c.get('max_volume') or 1
        current = c.get('current_level') or 0
        try:
            percent = round((current / float(max_vol)) * 100, 1)
        except Exception:
            percent = 0.0

        f = forecast_map.get(c.get('id'))
        time_to_full_min = f.get('time_to_full_min') if f else None
        # Provide seconds-level precision and a server timestamp so clients can sync
        time_to_full_seconds = None
        if time_to_full_min is not None:
            try:
                time_to_full_seconds = int(round(float(time_to_full_min) * 60))
            except Exception:
                time_to_full_seconds = None

        status = c.copy()
        status['percent_full'] = percent
        status['time_to_full_min'] = time_to_full_min
        status['time_to_full_seconds'] = time_to_full_seconds
        # as_of timestamp (UTC) so client can align countdowns
        try:
            now_utc = datetime.utcnow().replace(tzinfo=timezone.utc)
            status['as_of'] = now_utc.isoformat()
            if time_to_full_seconds is not None:
                try:
                    # compute proposed full_at
                    final_full_at = now_utc + timedelta(seconds=int(time_to_full_seconds))
                    
                    # *** BUG FIX: REMOVED THE FLAWED "SMOOTHING POLICY" ***
                    # The client-side logic is now the only source of truth
                    # for the 1-second countdown, so we just send the
                    # most accurate 'full_at' time we have.
                    
                    status['full_at'] = final_full_at.isoformat()
                except Exception:
                    status['full_at'] = None
            else:
                status['full_at'] = None
        except Exception:
            status['as_of'] = None
            status['full_at'] = None
        status_list.append(status)

    return jsonify(status_list)


@app.route('/api/data/historic')
def data_historic():
    """Return historic /api/Data records filtered by query params:
    - start: ISO date (inclusive)
    - end: ISO date (inclusive)
    - cauldron_id: optional, filter to a single cauldron's level map
    """
    start_q = request.args.get('start')
    end_q = request.args.get('end')
    cauldron_id = request.args.get('cauldron_id')

    raw = safe_get(EOG_API_BASE_URL + '/api/Data')
    if raw is None:
        return jsonify({'error': 'Could not fetch /api/Data (timeout or API error)'}), 500

    # Normalize wrapper shapes
    data_list = raw if isinstance(raw, list) else (raw.get('data') if isinstance(raw, dict) and isinstance(raw.get('data'), list) else [])

    # Parse filter times
    start_dt = None
    end_dt = None
    if start_q:
        try:
            start_dt = _parse_timestamp(start_q if 'T' in start_q else start_q + 'T00:00:00')
        except Exception:
            start_dt = None
    if end_q:
        try:
            end_dt = _parse_timestamp(end_q if 'T' in end_q else end_q + 'T23:59:59')
        except Exception:
            end_dt = None

    out = []
    for rec in data_list:
        ts = None
        if isinstance(rec, dict):
            ts = _parse_timestamp(rec.get('timestamp') or rec.get('time') or rec.get('t'))
        if ts is None:
            continue
        if start_dt and ts < start_dt:
            continue
        if end_dt and ts > end_dt:
            continue

        if cauldron_id:
            # filter to a single cauldron's numeric value
            levels = rec.get('cauldron_levels') or rec.get('levels') or {}
            value = None
            if isinstance(levels, dict):
                value = levels.get(cauldron_id)
            out.append({'timestamp': ts.isoformat(), 'cauldron_id': cauldron_id, 'value': value})
        else:
            out.append(rec)

    return jsonify(out)


@app.route('/api/network')
def get_network():
    """Return static factory network + cauldron positions for frontend visualization."""
    return jsonify(factory_static_data)


def _extract_ticket_amount(ticket):
    for k in ('amount', 'amount_collected', 'quantity', 'volume'):
        v = ticket.get(k)
        if v is not None:
            try:
                return float(v)
            except Exception:
                pass
    # fallback: look for nested fields
    for v in ticket.values():
        if isinstance(v, (int, float)):
            return float(v)
    return None


@app.route('/api/tickets/match')
def tickets_match():
    """Match end-of-day tickets to drain events using historical /api/Data.

    Returns a list of ticket match results and any unmatched drain events.
    This recomputes on each request so it is resilient to changing ticket input.
    """
    tickets_raw = safe_get(EOG_API_BASE_URL + '/api/Tickets')
    if tickets_raw is None:
        return jsonify({'error': 'Could not fetch /api/Tickets (timeout or API error)'}), 500

    # normalize tickets list
    tickets_list = tickets_raw if isinstance(tickets_raw, list) else (tickets_raw.get('transport_tickets') if isinstance(tickets_raw, dict) and isinstance(tickets_raw.get('transport_tickets'), list) else (tickets_raw.get('tickets') if isinstance(tickets_raw, list) else []))

    # Fetch full historical data once
    data_raw = safe_get(EOG_API_BASE_URL + '/api/Data')
    if data_raw is None:
        return jsonify({'error': 'Could not fetch /api/Data (timeout or API error)'}), 500

    data_list = data_raw if isinstance(data_raw, list) else (data_raw.get('data') if isinstance(data_raw, dict) and isinstance(data_raw.get('data'), list) else [])

    # Build per-cauldron time series map
    series_map = {}
    for rec in data_list:
        ts = _parse_timestamp(rec.get('timestamp') or rec.get('time') or rec.get('t'))
        if ts is None:
            continue
        levels = rec.get('cauldron_levels') or rec.get('levels') or {}
        if not isinstance(levels, dict):
            continue
        for cid, val in levels.items():
            try:
                v = float(val)
            except Exception:
                continue
            series_map.setdefault(cid, []).append((ts, v))

    # sort series
    for cid in series_map:
        series_map[cid].sort(key=lambda x: x[0])

    results = []
    unmatched_drains = []

    # Helper: find static cauldron data
    def get_static(cauldron_id):
        return next((c for c in factory_static_data['cauldrons'] if c['id'] == cauldron_id), None)

    # Precompute all drain events per cauldron by day
    drains_by_cauldron_day = {}
    for cid, series in series_map.items():
        static = get_static(cid)
        fill_rate = static.get('fill_rate_per_min', 0) if static else 0
        # iterate and group consecutive decreases into events
        i = 0
        n = len(series)
        while i < n-1:
            t0, v0 = series[i]
            j = i+1
            # look for decrease
            if series[j][1] < v0:
                start_t = t0
                start_v = v0
                end_t = series[j][0]
                end_v = series[j][1]
                j += 1
                while j < n and series[j][1] < series[j-1][1]:
                    end_t = series[j][0]
                    end_v = series[j][1]
                    j += 1

                duration_min = (end_t - start_t).total_seconds() / 60.0
                drained = max(0.0, start_v - end_v)
                # account for potion generated during drain
                drained_adjusted = drained + (fill_rate * duration_min)

                day_key = start_t.date().isoformat()
                drains_by_cauldron_day.setdefault(cid, {}).setdefault(day_key, []).append({
                    'start': start_t.isoformat(),
                    'end': end_t.isoformat(),
                    'start_v': start_v,
                    'end_v': end_v,
                    'duration_min': round(duration_min, 1),
                    'drained': round(drained_adjusted, 2)
                })
                i = j
            else:
                i += 1

    # Now match tickets
    for t in tickets_list:
        # normalize ticket fields
        ticket_id = t.get('id') or t.get('ticket_id') or t.get('ticketId')
        cauldron_id = t.get('cauldronId') or t.get('cauldron_id') or t.get('cauldron') or t.get('cauldronId')
        # date may be just a date string
        date_str = t.get('date') or t.get('day') or t.get('ticket_date') or t.get('timestamp')
        amount = _extract_ticket_amount(t)

        # tolerant parsing of date
        match_day = None
        if date_str:
            try:
                # if only date like YYYY-MM-DD
                if len(date_str) <= 10:
                    match_day = datetime.fromisoformat(date_str).date().isoformat()
                else:
                    dt = _parse_timestamp(date_str)
                    if dt:
                        match_day = dt.date().isoformat()
            except Exception:
                match_day = None

        calculated = None
        matched_events = []
        
        # *** BUG FIX 1: 'cid' was used here, but it should be 'cauldron_id' ***
        if cauldron_id and match_day and cauldron_id in drains_by_cauldron_day:
            # use our drains_by_cauldron_day lookup
            day_drains = drains_by_cauldron_day.get(cauldron_id, {}).get(match_day, [])
            calculated = sum(d['drained'] for d in day_drains)
            matched_events = day_drains

        # If we couldn't compute from events, fallback to per-sample diff sum
        if calculated is None and cauldron_id:
            # try naive computation over series_map
            series = series_map.get(cauldron_id, [])
            # sum all decreases within that calendar day
            if match_day:
                s = 0.0
                for i in range(len(series)-1):
                    a_ts, a_v = series[i]
                    b_ts, b_v = series[i+1]
                    if a_ts.date().isoformat() != match_day:
                        continue
                    if b_ts.date().isoformat() != match_day:
                        continue
                    if b_v < a_v:
                        s += (a_v - b_v)
                calculated = s

        # Determine suspicious: absolute diff > 5L and >10% of ticket
        suspicious = False
        diff = None
        reason = ''
        if amount is not None and calculated is not None:
            diff = round(amount - calculated, 2)
            if abs(diff) > 5 and abs(diff) > 0.1 * max(1.0, amount):
                suspicious = True
                reason = f'Difference {diff}L exceeds threshold.'
        else:
            reason = 'Insufficient data to compute match.'

        # Log a concise summary to help debugging in judge runs
        app.logger.info(f"[tickets_match] ticket={ticket_id} cauldron={cauldron_id} day={match_day} ticket_amount={amount} calculated={calculated} diff={diff} suspicious={suspicious}")

        results.append({
            'ticket_id': ticket_id,
            'cauldron_id': cauldron_id,
            'ticket_amount': amount,
            'calculated_amount': None if calculated is None else round(calculated,2),
            'difference': diff,
            'suspicious': suspicious,
            'matched_events': matched_events,
            'reason': reason
        })

    # find drain events that have no matching ticket (unmatched drains)
    for cid, days in drains_by_cauldron_day.items():
        for day, events in days.items():
            # if there is no ticket for this cauldron/day, mark as unlogged
            has_ticket = any((r for r in results if r['cauldron_id'] == cid and r['ticket_amount'] is not None and r['ticket_id'] is not None and (r['calculated_amount'] is not None)))
            if not has_ticket:
                for e in events:
                    unmatched_drains.append({'cauldron_id': cid, 'day': day, 'event': e})

    return jsonify({'matches': results, 'unmatched_drains': unmatched_drains})

@app.route('/api/logistics/dispatch_courier', methods=['POST'])
def dispatch_courier():
    """
    Tool (NVIDIA Action): Dispatches a courier witch.
    This is a simulation, as there is no POST endpoint in your screenshot.
    """
    data = request.json
    cauldron_id = data.get('cauldron_id')
    
    cauldron_data = next((c for c in factory_static_data['cauldrons'] if c['id'] == cauldron_id), None)
    
    if not cauldron_data:
        return jsonify({"status": "error", "message": "Invalid cauldron ID."}), 400
    
    print(f"SIMULATED DISPATCH: Courier sent to {cauldron_data['name']}")
    
    return jsonify({
        "status": "success",
        "message": f"Courier witch dispatched to {cauldron_data['name']}. (Simulation)"
    })


# --- NVIDIA Challenge: The Agent "Brain" (Controller) ---
# This part stays exactly the same! It just calls our tools.

@app.route('/api/agent/chat', methods=['POST'])
def handle_agent_chat():
    user_message = request.json.get('message')
    # Optional: the client can pass `nv_api_key` or set NV_API_KEY (or nv_api_key) env var.
    nv_api_key = request.json.get('nv_api_key') or _get_nv_api_key_from_env()
    use_nemotron = bool(request.json.get('use_nemotron')) or bool(nv_api_key)
    # Control whether Nemotron's internal 'reasoning' fragments are exposed in responses
    show_reasoning = bool(request.json.get('debug')) or bool(os.environ.get('NV_SHOW_REASONING'))
    
    agent_plan = [] 
    agent_final_response = ""

    # *** THIS IS THE FIX: REMOVED THE THREADED HELPER ***
    # Helper function to safely call and parse tools
    def _call_and_extract(fn):
        """
        Calls the tool function directly *in the same thread*
        to preserve the Flask application context.
        """
        try:
            # Call the function directly
            res = fn()
            
            # If it's a tuple like (resp, status)
            if isinstance(res, tuple):
                    res = res[0]
            # If it's a Flask Response object
            if hasattr(res, 'get_json'):
                try:
                    return res.get_json()
                except Exception:
                    return {'error': 'Tool returned non-JSON response'}
            # If it's already a dict/list
            if isinstance(res, (dict, list)):
                return res
            # Last resort
            try:
                return json.loads(res.data)
            except Exception:
                return {'error': 'Could not decode tool response'}
                
        except Exception as e:
            # Catch any exception from the tool itself
            tb = traceback.format_exc() # Get the full traceback
            print(f"--- Exception in tool {fn.__name__} ---")
            print(tb)
            print("---------------------------------------")
            return {'error': f'Exception in tool: {e}'}


    # REAL EOG LOGIC 1: User asks for anomalies
    if "suspicious" in user_message.lower() or "anomaly" in user_message.lower() or "ticket" in user_message.lower():
        agent_plan.append("Plan: User asked about discrepancies. I will call the real `tickets_match()` tool.")
        
        match_data = _call_and_extract(tickets_match) 
        
        suspicious_matches = [m for m in match_data.get('matches', []) if m['suspicious']]
        unmatched_drains = match_data.get('unmatched_drains', [])
        
        if suspicious_matches or unmatched_drains:
            agent_plan.append("Tool Result: Found discrepancies.")
            agent_final_response = "I've checked the tickets and found some problems:\n"
            for match in suspicious_matches:
                agent_final_response += f"  - Suspicious Ticket: {match['ticket_id']} ({match['cauldron_id']}). Reason: {match['reason']}\n"
            for drain in unmatched_drains:
                agent_final_response += f"  - Unmatched Drain: {drain['cauldron_id']} on {drain['day']} for {drain['event']['drained']:.1f}L.\n"
        else:
            agent_plan.append("Tool Result: No discrepancies found.")
            agent_final_response = "I've checked all tickets against the historical data. All potion flows are accounted for."

    # SIMULATION 2: User asks for forecasts
    elif "forecast" in user_message.lower() or "full" in user_message.lower():
        agent_plan.append("Plan: User asked for forecasts. I will call `forecast_fill_times()`.")

        # This calls our tool, which calls /api/Data
        try:
            forecasts = _call_and_extract(forecast_fill_times)
        except Exception as e:
            forecasts = {'error': str(e)} # Be explicit that an error is a dict

        agent_plan.append(f"Tool Result: {forecasts}")

        # Check if 'forecasts' is a list before trying to sort it.
        if isinstance(forecasts, list):
            # If user asked about a particular cauldron ("how full is amber glow"), try to match name first
            agent_final_response = "Here is the live forecast (top 5):\n"

            # Helper: case-insensitive substring match against static cauldron names/ids
            def _find_cauldron_by_text(text):
                if not text:
                    return None
                q = text.lower()
                for c in factory_static_data.get('cauldrons', []):
                    name = (c.get('name') or '').lower()
                    cid = (c.get('id') or '').lower()
                    if q in name or q in cid:
                        return c
                return None

            # Try to parse a potential cauldron name from the user message by stripping common words
            possible_name = None
            # crude heuristic: take words after 'is' or 'how full is' up to 5 words
            low = user_message.lower()
            if 'how full is' in low:
                possible_name = low.split('how full is',1)[1].strip().split('?')[0].strip()
            elif 'how full' in low and 'is' in low:
                parts = low.split('is',1)
                possible_name = parts[1].strip()

            matched_cauldron = _find_cauldron_by_text(possible_name) if possible_name else None

            # Sort and report top 5 forecasts as before
            forecasts.sort(key=lambda x: x.get('time_to_full_min', 9999))
            for f in forecasts[:5]:
                agent_final_response += f"  - {f.get('name','?')} ({f.get('cauldron_id','?')}) will be full in {f.get('time_to_full_min','?')} minutes.\n"

            # If a specific cauldron was matched, give its live percent_full and time-to-full if available
            if matched_cauldron:
                try:
                    status_data = _call_and_extract(cauldron_status)
                except Exception:
                    status_data = None

                found = None
                if isinstance(status_data, list):
                    for s in status_data:
                        # match by id or name
                        if s.get('id') == matched_cauldron.get('id') or (s.get('name') or '').lower() == (matched_cauldron.get('name') or '').lower():
                            found = s
                            break

                if found:
                    pct = found.get('percent_full')
                    ttf_min = found.get('time_to_full_min')
                    agent_final_response += f"\n{matched_cauldron.get('name')} ({matched_cauldron.get('id')}) is {pct}% full."
                    if ttf_min is not None:
                        agent_final_response += f" Estimated time to full: {ttf_min} minutes.\n"
                    else:
                        agent_final_response += " No reliable time-to-full available.\n"
                else:
                    agent_final_response += f"\nI found '{possible_name}' but couldn't fetch live status; try again or provide the cauldron id.\n"
        else:
            # It's an error dictionary
            error_message = forecasts.get('error', 'Unknown error')
            agent_final_response = f"I couldn't get the forecast. The tool reported an error: {error_message}"
            agent_plan.append(f"Error: {error_message}")

    # SIMULATION 3: User wants to TAKE ACTION
    elif "dispatch" in user_message.lower() or "empty" in user_message.lower():
        cauldron_id_to_dispatch = None
        for cauldron in factory_static_data['cauldrons']:
            if cauldron['id'] in user_message.lower() or cauldron['name'].split(" ")[0].lower() in user_message.lower():
                cauldron_id_to_dispatch = cauldron['id']
                break
        
        if cauldron_id_to_dispatch:
            agent_plan.append(f"Plan: User wants to dispatch to {cauldron_id_to_dispatch}. I will call `dispatch_courier()`.")
            
            # This makes a POST request to our *own* server (use safe_post with short timeout)
            dispatch_result = safe_post(
                "http://127.0.0.1:5000/api/logistics/dispatch_courier",
                json={"cauldron_id": cauldron_id_to_dispatch},
                timeout=3
            ) or {"status": "error", "message": "dispatch failed or timed out"}

            agent_plan.append(f"Tool Result: {dispatch_result.get('status')}.")
            agent_final_response = dispatch_result.get('message', 'No message')
        else:
            agent_final_response = "Which cauldron (e.g., cauldron_001) should I dispatch to?"
            
    # SIMULATION 4: User asks for the BONUS
    elif "optimize" in user_message.lower() or "routes" in user_message.lower() or "witches" in user_message.lower():
        agent_plan.append("Plan: User asked for the Bonus. I will explain the solution using the live API data.")
        
        # Pull data from our loaded static info!
        network_edges = len(factory_static_data['network']) # This might be a dict, adjust as needed
        num_couriers = len(factory_static_data['couriers'])
        market_name = factory_static_data['market'].get('name', 'The Enchanted Market')
        
        agent_final_response = (
            "This is the EOG Bonus! Here is how I would solve it:\n"
            f"1. **Use Forecast:** First, I call my `forecast_fill_times()` tool to get a 'deadline' for each cauldron.\n"
            f"2. **Use Network Map:** I will use the **`/api/Information/network`** data to calculate travel times between the {market_name} and all urgent cauldrons.\n"
            f"3. **Account for Constraints:** I'll add the 15-minute `unload_time` at the market, plus the `drain_rate` (from `/api/Data/metadata`) to calculate drain time.\n"
            f"4. **Find Minimum Witches:** I'll run a VRP (Vehicle Routing Problem) algorithm to find the minimum number of the **{num_couriers} available couriers** (from `/api/Information/couriers`) needed to service all cauldrons before they overflow."
        )

    else:
        agent_final_response = "I am connected to the EOG API. I can **check tickets**, **forecast** fill times, **dispatch** couriers, or **optimize routes**."
    # If requested, refine or generate the final response using NVIDIA Nemotron
    if use_nemotron:
        if not _HAS_NEMOTRON:
            agent_plan.append("Note: Nemotron client not installed; set up 'openai' package to enable.")
        elif not nv_api_key:
            agent_plan.append("Note: NV API key not provided; set 'nv_api_key' in the request or NV_API_KEY env var.")
        else:
            # Stream from Nemotron and assemble the final response server-side.
            try:
                system_msg = (
                    "You are an assistant integrated with a factory monitoring system. "
                    "Use the agent plan and tool outputs to craft a concise, actionable reply to the user. "
                    "Be clear about any suggested actions."
                )

                context_text = "\n".join(agent_plan)
                prompt = (
                    f"Context:\n{context_text}\n\nUser message:\n{user_message}\n\n"
                    "Provide a short assistant reply based on the context."
                )

                messages = [
                    {"role": "system", "content": system_msg},
                    {"role": "user", "content": prompt}
                ]

                client = OpenAI(base_url="https://integrate.api.nvidia.com/v1", api_key=nv_api_key)
                completion = client.chat.completions.create(
                    model="nvidia/nvidia-nemotron-nano-9b-v2",
                    messages=messages,
                    temperature=0.6,
                    top_p=0.95,
                    max_tokens=512,
                    frequency_penalty=0,
                    presence_penalty=0,
                    stream=True,
                    extra_body={"min_thinking_tokens": 256, "max_thinking_tokens": 512}
                )

                assembled = []
                reasoning_parts = []
                # iterate streamed deltas and collect content
                for chunk in completion:
                    try:
                        delta = chunk.choices[0].delta
                    except Exception:
                        delta = None

                    if delta is None:
                        continue

                    reasoning = getattr(delta, 'reasoning_content', None)
                    content = getattr(delta, 'content', None)
                    if content is None:
                        content = getattr(delta, 'text', None)

                    if reasoning:
                        reasoning_parts.append(str(reasoning))
                    if content:
                        assembled.append(str(content))

                final_text = "".join(assembled).strip()
                if final_text:
                    agent_final_response = final_text
                    agent_plan.append("Tool Result: Response generated by Nemotron (stream).")
                    # Optionally attach reasoning to the plan for debugging
                    if reasoning_parts and show_reasoning:
                        agent_plan.append("Nemotron reasoning: " + " ".join(reasoning_parts))
                else:
                    agent_plan.append("Warning: Nemotron streamed no text; keeping local response.")
            except Exception as e:
                # Log full traceback to server logs for debugging connectivity/disconnect issues
                tb = traceback.format_exc()
                print("[Nemotron] streaming call failed:", str(e))
                print(tb)
                agent_plan.append(f"Nemotron call failed (stream): {str(e)}")

                # Attempt a one-time non-streaming fallback to get an error message or final text
                try:
                    fallback = client.chat.completions.create(
                        model="nvidia/nvidia-nemotron-nano-9b-v2",
                        messages=messages,
                        temperature=0.6,
                        top_p=0.95,
                        max_tokens=512,
                        frequency_penalty=0,
                        presence_penalty=0,
                        stream=False,
                        extra_body={"min_thinking_tokens": 256, "max_thinking_tokens": 512}
                    )
                    # Try to extract returned text from several possible shapes
                    text_out = ""
                    try:
                        if hasattr(fallback, 'choices'):
                            ch0 = fallback.choices[0]
                            # OpenAI-like SDKs sometimes put content in .message or .text
                            if hasattr(ch0, 'message') and isinstance(ch0.message, dict) and 'content' in ch0.message:
                                text_out = ch0.message['content']
                            elif hasattr(ch0, 'text'):
                                text_out = ch0.text
                            elif isinstance(ch0, dict):
                                msg = ch0.get('message') or {}
                                text_out = msg.get('content') or ch0.get('text') or ""
                        elif isinstance(fallback, dict):
                            choices = fallback.get('choices', [])
                            if choices:
                                c0 = choices[0]
                                msg = c0.get('message') or {}
                                text_out = msg.get('content') or c0.get('text') or ""
                    except Exception as e_parse:
                        agent_plan.append(f"Nemotron fallback parse failed: {e_parse}")

                    text_out = (text_out or "").strip()
                    if text_out:
                        agent_final_response = text_out
                        agent_plan.append("Tool Result: Nemotron non-streaming response used as fallback.")
                    else:
                        agent_plan.append("Nemotron non-streaming returned empty text.")
                except Exception as e2:
                    print("[Nemotron] non-stream fallback failed:", str(e2))
                    agent_plan.append(f"Nemotron non-stream fallback failed: {str(e2)}")

    return jsonify({
        "agent_response": agent_final_response,
        "agent_plan": agent_plan
    })


# --- Frontend Routes ---
@app.route('/')
def index():
    """Serves the new homepage."""
    # Serve the index.html from the project's root folder
    return send_from_directory(app.root_path, 'index.html')

@app.route('/dashboard')
def dashboard():
    """Serves the main Poyolab dashboard app."""
    # Serve the dashboard.html from the project's root folder
    return send_from_directory(app.root_path, 'dashboard.html')


@app.route('/api/time')
def api_time():
    """Return the server UTC time for client clock synchronization."""
    try:
        now = datetime.utcnow().replace(tzinfo=timezone.utc).isoformat()
        return jsonify({'server_time': now})
    except Exception:
        return jsonify({'server_time': None})


def _build_graph_from_network(network_obj):
    """Build adjacency map from network information.
    Accepts several shapes: list of edges, or {edges: [...]}
    Each edge may have 'from'/'to' or 'u'/'v' and a travel_time or travel_time_minutes field.
    Returns: { node: [(neighbor, travel_seconds), ...], ... }
    """
    adj = {}
    edges = []
    if isinstance(network_obj, dict):
        # common wrappers
        if 'edges' in network_obj and isinstance(network_obj['edges'], list):
            edges = network_obj['edges']
        elif 'network' in network_obj and isinstance(network_obj['network'], dict) and isinstance(network_obj['network'].get('edges'), list):
            edges = network_obj['network']['edges']
        else:
            # maybe it's already a list-like dict
            edges = []
    elif isinstance(network_obj, list):
        edges = network_obj

    for e in edges:
        if not isinstance(e, dict):
            continue
        a = e.get('from') or e.get('src') or e.get('u') or e.get('a')
        b = e.get('to') or e.get('dst') or e.get('v') or e.get('b')
        if not a or not b:
            continue
        # determine travel time in seconds
        t = None
        for k in ('travel_time_minutes','travel_time_min','travel_time','time_minutes','time','cost'):
            if k in e:
                try:
                    t = float(e[k])
                    break
                except Exception:
                    continue
        if t is None:
            # default small travel (1 minute)
            t = 1.0
        # if field was in minutes convert to seconds where key name suggests minutes
        if 'minute' in (',').join(list(e.keys())).lower() or 'min' in (',').join(list(e.keys())).lower():
            # assume minutes
            t_sec = int(round(float(t) * 60))
        else:
            # ambiguous: treat as minutes by default
            t_sec = int(round(float(t) * 60))

        adj.setdefault(a, []).append((b, t_sec))
        adj.setdefault(b, []).append((a, t_sec))

    return adj


def _dijkstra(adj, source):
    import heapq
    dist = {source: 0}
    prev = {}
    h = [(0, source)]
    while h:
        d, u = heapq.heappop(h)
        if d != dist.get(u, None):
            continue
        for v, w in adj.get(u, []):
            nd = d + w
            if v not in dist or nd < dist[v]:
                dist[v] = nd
                prev[v] = u
                heapq.heappush(h, (nd, v))
    return dist, prev


@app.route('/api/optimizer/compute')
def api_optimizer_compute():
    """Compute courier routes to prevent cauldron overflow.
    Returns a greedy schedule and the minimal number of couriers found by packing.
    Query params:
      unload_min (default 15)
      safety_min (default 0)
    """
    try:
        unload_min = float(request.args.get('unload_min') or 15.0)
        safety_min = float(request.args.get('safety_min') or 0.0)
    except Exception:
        unload_min = 15.0
        safety_min = 0.0

    # get live status
    try:
        status_resp = cauldron_status()
        if hasattr(status_resp, 'get_json'):
            status = status_resp.get_json()
        else:
            status = status_resp
    except Exception as e:
        return jsonify({'error': f'Could not compute status: {e}'}), 500

    # Build list of tasks: cauldrons with time_to_full_seconds
    now = datetime.utcnow().replace(tzinfo=timezone.utc)
    tasks = []
    for s in (status or []):
        try:
            cid = s.get('id')
            ttf = s.get('time_to_full_seconds')
            pct = s.get('percent_full') or 0
            if ttf is None:
                continue
            deadline = now + timedelta(seconds=int(ttf) - int(safety_min * 60))
            tasks.append({'id': cid, 'deadline': deadline, 'ttf_seconds': int(ttf), 'percent_full': pct})
        except Exception:
            continue

    # load network adjacency
    network_obj = factory_static_data.get('network')
    adj = _build_graph_from_network(network_obj)

    market = factory_static_data.get('market') or {}
    market_id = market.get('id') or market.get('name') or 'market'

    # precompute shortest paths among market and task nodes to speed feasibility checks
    nodes_of_interest = [market_id] + [t['id'] for t in tasks]
    dist_matrix = {}
    for src in nodes_of_interest:
        dists, _ = _dijkstra(adj, src)
        dist_matrix[src] = dists

    # map id->task
    task_map = {t['id']: t for t in tasks}
    # sort by deadline ascending
    tasks.sort(key=lambda x: x['deadline'])

    routes = []  # each route: {'seq':[ids], 'arrivals':{id:datetime}, 'impossible':bool}

    def travel_seconds(u, v):
        if u in dist_matrix and v in dist_matrix[u]:
            return dist_matrix[u][v]
        # fallback to dijkstra on-the-fly
        dists, _ = _dijkstra(adj, u)
        return dists.get(v)

    def simulate_route_with_seq(seq):
        # simulate starting at market now, visiting seq in order; return (feasible:boolean, arrivals:dict)
        tcur = now
        arrivals = {}
        prev = market_id
        for node in seq:
            travel = travel_seconds(prev, node)
            if travel is None:
                return False, None
            tcur = tcur + timedelta(seconds=travel)
            arrivals[node] = tcur
            prev = node
        # return to market
        travel_back = travel_seconds(prev, market_id)
        if travel_back is None:
            return False, None
        tcur = tcur + timedelta(seconds=travel_back)
        # include unload time at market
        tcur = tcur + timedelta(minutes=unload_min)
        # check deadlines
        for node, at in arrivals.items():
            task = task_map.get(node)
            if task and at > task['deadline']:
                return False, None
        return True, arrivals

    # Insertion-based greedy: try to insert each task into existing routes at best position
    for task in tasks:
        placed = False
        best_choice = None  # (route_idx, insert_pos, arrivals)
        for ri, r in enumerate(routes):
            seq = r['seq']
            # try all insertion positions 0..len(seq)
            for pos in range(0, len(seq)+1):
                new_seq = seq[:pos] + [task['id']] + seq[pos:]
                ok, arrivals = simulate_route_with_seq(new_seq)
                if ok:
                    # choose the insertion that yields earliest latest-arrival (tightest)
                    latest_arrival = max(arrivals.values()) if arrivals else now
                    if best_choice is None or latest_arrival < best_choice[3]:
                        best_choice = (ri, pos, new_seq, latest_arrival, arrivals)
        if best_choice:
            ri, pos, new_seq, _, arrivals = best_choice
            routes[ri]['seq'] = new_seq
            routes[ri]['arrivals'] = arrivals
            placed = True
        if not placed:
            # try to create a new single-task route
            ok, arrivals = simulate_route_with_seq([task['id']])
            if ok:
                routes.append({'seq': [task['id']], 'arrivals': arrivals, 'impossible': False})
            else:
                # cannot service even alone -> impossible
                routes.append({'seq': [task['id']], 'arrivals': {}, 'impossible': True})

    # format response
    resp_routes = []
    for i, r in enumerate(routes):
        resp_routes.append({
            'courier': i+1,
            'sequence': r['seq'],
            'arrivals': {k: v.isoformat() for k, v in (r.get('arrivals') or {}).items()},
            'impossible': bool(r.get('impossible', False))
        })

    return jsonify({
        'required_couriers': len(routes),
        'routes': resp_routes,
        'now': now.isoformat()
    })

if __name__ == '__main__':
    app.run(debug=True, port=5000)