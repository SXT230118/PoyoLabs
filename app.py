from flask import Flask, jsonify, request, render_template, send_from_directory, redirect, url_for, session
from flask_cors import CORS
try:
    from authlib.integrations.flask_client import OAuth
    _HAVE_AUTHLIB = True
except Exception:
    OAuth = None
    _HAVE_AUTHLIB = False
from functools import wraps
from urllib.parse import quote_plus, urlencode
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

# Global state for courier dispatch operations
active_drains = {}  # {cauldron_id: {'start_time': datetime, 'initial_level': float, 'drain_rate': float}}
drains_lock = threading.Lock()  # Thread safety for active_drains
resolved_tickets = set()  # Track ticket IDs that have been resolved (locally)
resolved_tickets_lock = threading.Lock()  # Thread safety for resolved_tickets

# --- Agent System: Multi-step workflow with tool integration ---
class AgentWorkflow:
    """
    Multi-step agent system that plans, executes tools, and responds.
    Demonstrates NVIDIA requirements:
    1. Beyond chatbot - complex workflows with planning
    2. Multi-step - plan → execute → reflect → respond
    3. Tool integration - calls external APIs intelligently
    4. Real-world applicability - solves actual factory monitoring problems
    
    ENHANCED FEATURES:
    - Proactive monitoring and alerts
    - Trend analysis and predictions
    - Autonomous recommendations
    - Multi-cauldron coordination
    """
    def __init__(self, nemotron_client=None):
        self.client = nemotron_client
        self.conversation_history = []
        self.last_alert_time = {}
        self.alert_cooldown = 300  # 5 minutes between similar alerts
        self.last_suggestion = None  # Track last suggested action
        self.pending_action = None  # Track pending action awaiting confirmation
        self.tools = {
            'check_tickets': self._check_tickets,
            'forecast_fills': self._forecast_fills,
            'get_status': self._get_status,
            'dispatch_courier': self._dispatch_courier,
            'dispatch_bulk': self._dispatch_bulk,
            'optimize_routes': self._optimize_routes,
            'analyze_network': self._analyze_network,
            'detect_anomalies': self._detect_anomalies,
            'analyze_trends': self._analyze_trends,
            'suggest_actions': self._suggest_actions,
            'compare_performance': self._compare_performance
        }
    
    def _check_tickets(self):
        """Tool: Analyze tickets for discrepancies"""
        try:
            res = tickets_match()
            if res.status_code == 200:
                return res.get_json()
            return {'error': f'HTTP {res.status_code}'}
        except Exception as e:
            return {'error': str(e)}
    
    def _forecast_fills(self):
        """Tool: Get fill time forecasts"""
        try:
            return forecast_fill_times()
        except Exception as e:
            return {'error': str(e)}
    
    def _get_status(self):
        """Tool: Get current cauldron status"""
        try:
            res = cauldron_status()
            if res.status_code == 200:
                data = res.get_json()
                
                # Annotate with drain status for clarity
                global active_drains
                if isinstance(data, list):
                    for cauldron in data:
                        cid = cauldron.get('id')
                        if cid in active_drains:
                            drain_info = active_drains[cid]
                            cauldron['drain_status'] = {
                                'active': True,
                                'started': drain_info['start_time'].isoformat(),
                                'progress': cauldron.get('drain_progress', 0)
                            }
                
                return data
            return {'error': f'HTTP {res.status_code}'}
        except Exception as e:
            return {'error': str(e)}
    
    def _resolve_tickets_for_cauldron(self, cauldron_id):
        """Mark tickets as resolved locally (and optionally try API if available)"""
        global resolved_tickets, resolved_tickets_lock
        
        try:
            # Get all unresolved tickets for this cauldron
            tickets_raw = safe_get(EOG_API_BASE_URL + '/api/Tickets')
            if not tickets_raw:
                print(f"[RESOLVE] Could not fetch tickets for {cauldron_id}")
                return
            
            tickets_list = tickets_raw if isinstance(tickets_raw, list) else (
                tickets_raw.get('transport_tickets') if isinstance(tickets_raw, dict) else []
            )
            
            resolved_count = 0
            with resolved_tickets_lock:
                for ticket in tickets_list:
                    ticket_cauldron = ticket.get('cauldronId') or ticket.get('cauldron_id') or ticket.get('cauldron')
                    ticket_id = ticket.get('id') or ticket.get('ticket_id') or ticket.get('ticketId')
                    
                    # Mark all tickets for this cauldron as resolved locally
                    if ticket_cauldron == cauldron_id and ticket_id and ticket_id not in resolved_tickets:
                        resolved_tickets.add(ticket_id)
                        resolved_count += 1
                        print(f"[RESOLVE] ✓ Locally resolved ticket {ticket_id} for {cauldron_id}")
                        
                        # Optionally try to resolve via API (but don't fail if it doesn't work)
                        try:
                            url = f"{EOG_API_BASE_URL}/api/Tickets/{ticket_id}"
                            requests.put(url, json={'status': 'resolved'}, timeout=2, verify=False)
                        except:
                            pass  # Ignore API errors - we track locally
            
            if resolved_count > 0:
                print(f"[RESOLVE] ✅ Marked {resolved_count} ticket(s) as resolved for {cauldron_id}")
            else:
                print(f"[RESOLVE] No new tickets to resolve for {cauldron_id}")
                
        except Exception as e:
            print(f"[RESOLVE] Error in _resolve_tickets_for_cauldron: {e}")
    
    def _dispatch_courier(self, cauldron_id):
        """Tool: Dispatch courier to cauldron - initiates gradual draining"""
        if not cauldron_id:
            return {'error': 'No cauldron ID provided', 'status': 'failed'}
        
        try:
            # Validate cauldron exists in static data
            cauldron_static = next((c for c in factory_static_data['cauldrons'] if c['id'] == cauldron_id), None)
            
            if not cauldron_static:
                return {
                    'error': f'Invalid cauldron ID: {cauldron_id}',
                    'status': 'failed'
                }
            
            # Get LIVE current level from status endpoint
            status_data = self._get_status()
            cauldron_live = next((c for c in status_data if c['id'] == cauldron_id), None) if isinstance(status_data, list) else None
            
            if not cauldron_live:
                return {
                    'error': f'Could not fetch live data for {cauldron_id}',
                    'status': 'failed'
                }
            
            current_level = cauldron_live.get('current_level', 0)
            max_volume = cauldron_live.get('max_volume', 1)
            # Use a realistic drain rate - couriers should drain faster than natural drain
            # If the cauldron's natural drain rate is too slow, use a minimum of 15 L/min for courier dispatch
            natural_drain = cauldron_static.get('drain_rate_per_min', 0)
            drain_rate = max(15.0, natural_drain)  # At least 15 L/min for courier operations
            
            # Check if already empty
            if current_level <= 0:
                return {
                    'status': 'success',
                    'message': f"{cauldron_static.get('name', cauldron_id)} is already empty - no drain needed",
                    'cauldron_id': cauldron_id,
                    'cauldron_name': cauldron_static.get('name', cauldron_id),
                    'current_level': 0,
                    'already_empty': True
                }
            
            # Check if already draining - don't restart the drain
            global active_drains, drains_lock
            
            with drains_lock:
                if cauldron_id in active_drains:
                    existing_drain = active_drains[cauldron_id]
                    elapsed = (datetime.now() - existing_drain['start_time']).total_seconds() / 60
                    drained_so_far = elapsed * existing_drain['drain_rate']
                    remaining = max(0, existing_drain['initial_level'] - drained_so_far)
                    progress = (drained_so_far / existing_drain['initial_level'] * 100) if existing_drain['initial_level'] > 0 else 100
                    
                    print(f"[DISPATCH] {cauldron_id} already draining: {progress:.1f}% complete, {remaining:.1f}L remaining")
                    
                    return {
                        'status': 'success',
                        'message': f"Courier already draining {cauldron_static.get('name', cauldron_id)}",
                        'cauldron_id': cauldron_id,
                        'cauldron_name': cauldron_static.get('name', cauldron_id),
                        'already_draining': True,
                        'current_level': remaining,
                        'initial_level': existing_drain['initial_level'],
                        'drain_progress': round(progress, 1),
                        'elapsed_minutes': round(elapsed, 1),
                        'drain_rate': existing_drain['drain_rate']
                    }
                
                # Start the drain operation
                print(f"[DISPATCH] Starting NEW drain for {cauldron_id}: {current_level:.1f}L at {drain_rate:.1f}L/min")
                active_drains[cauldron_id] = {
                'start_time': datetime.now(),
                'initial_level': current_level,
                'drain_rate': drain_rate,
                'cauldron_name': cauldron_static.get('name', cauldron_id)
            }
            
            # Calculate estimated completion time
            if drain_rate > 0:
                estimated_minutes = current_level / drain_rate
                completion_time = datetime.now() + timedelta(minutes=estimated_minutes)
            else:
                estimated_minutes = 0
                completion_time = datetime.now()
            
            print(f"[DISPATCH] Courier dispatched to {cauldron_static.get('name', cauldron_id)}")
            print(f"[DISPATCH] Draining {current_level:.1f}L at {drain_rate:.1f}L/min (~{estimated_minutes:.1f} min)")
            
            # Try to resolve any tickets for this cauldron
            try:
                self._resolve_tickets_for_cauldron(cauldron_id)
            except Exception as e:
                print(f"[DISPATCH] Warning: Could not resolve tickets for {cauldron_id}: {e}")
            
            return {
                'status': 'success',
                'message': f"Courier dispatched to {cauldron_static.get('name', cauldron_id)}",
                'cauldron_id': cauldron_id,
                'cauldron_name': cauldron_static.get('name', cauldron_id),
                'dispatched_at': datetime.now().isoformat(),
                'current_level': current_level,
                'max_volume': max_volume,
                'percent_full': (current_level / max_volume * 100) if max_volume > 0 else 0,
                'drain_rate': drain_rate,
                'estimated_completion': completion_time.isoformat(),
                'estimated_minutes': round(estimated_minutes, 1)
            }
            
        except Exception as e:
            return {
                'error': str(e),
                'status': 'failed',
                'cauldron_id': cauldron_id
            }
    
    def _dispatch_bulk(self, threshold=50):
        """Tool: Dispatch couriers to all cauldrons above a fill threshold"""
        try:
            # Get current status of all cauldrons
            status_data = self._get_status()
            if not isinstance(status_data, list):
                return {'error': 'Could not fetch cauldron status', 'status': 'failed'}
            
            # Filter cauldrons above threshold
            dispatched = []
            already_draining = []
            already_empty = []
            errors = []
            
            for cauldron in status_data:
                cauldron_id = cauldron.get('id')
                percent_full = cauldron.get('percent_full', 0)
                
                if percent_full >= threshold:
                    result = self._dispatch_courier(cauldron_id)
                    if result.get('status') == 'success':
                        if result.get('already_draining'):
                            already_draining.append({
                                'cauldron_id': cauldron_id,
                                'cauldron_name': result.get('cauldron_name'),
                                'percent_full': percent_full,
                                'progress': result.get('drain_progress', 0)
                            })
                        elif result.get('already_empty'):
                            already_empty.append({
                                'cauldron_id': cauldron_id,
                                'cauldron_name': result.get('cauldron_name')
                            })
                        else:
                            dispatched.append({
                                'cauldron_id': cauldron_id,
                                'cauldron_name': result.get('cauldron_name'),
                                'percent_full': percent_full,
                                'estimated_minutes': result.get('estimated_minutes', 0)
                            })
                    else:
                        errors.append({
                            'cauldron_id': cauldron_id,
                            'error': result.get('error', 'Unknown error')
                        })
            
            return {
                'status': 'success',
                'threshold': threshold,
                'total_dispatched': len(dispatched),
                'total_already_draining': len(already_draining),
                'total_already_empty': len(already_empty),
                'total_errors': len(errors),
                'dispatched': dispatched,
                'already_draining': already_draining,
                'already_empty': already_empty,
                'errors': errors
            }
            
        except Exception as e:
            return {
                'error': str(e),
                'status': 'failed'
            }
    
    def _optimize_routes(self):
        """Tool: Compute optimized courier routes"""
        try:
            # Call the optimizer endpoint via internal request
            res = safe_get("http://127.0.0.1:5000/api/optimizer/compute", timeout=5)
            return res or {'error': 'optimizer timeout'}
        except Exception as e:
            return {'error': str(e)}
    
    def _analyze_network(self):
        """Tool: Analyze network topology"""
        try:
            network = factory_static_data.get('network', {})
            cauldrons = factory_static_data.get('cauldrons', [])
            return {
                'network_size': len(network) if isinstance(network, dict) else 0,
                'total_cauldrons': len(cauldrons),
                'avg_capacity': sum(c.get('max_volume', 0) for c in cauldrons) / max(1, len(cauldrons))
            }
        except Exception as e:
            return {'error': str(e)}
    
    def _detect_anomalies(self):
        """Tool: Detect anomalies in cauldron behavior"""
        try:
            status_data = self._get_status()
            if isinstance(status_data, dict) and 'error' in status_data:
                return status_data
            
            anomalies = []
            for c in status_data:
                # Skip anomaly detection if actively draining
                if c.get('is_draining'):
                    continue
                
                # Check for unusual fill rates
                if c.get('percent_full', 0) > 95:
                    anomalies.append({
                        'cauldron': c.get('id'),
                        'type': 'critical_fill',
                        'severity': 'high',
                        'details': f"{c.get('percent_full')}% full"
                    })
                
                # Check for discrepancies
                if c.get('has_discrepancy'):
                    anomalies.append({
                        'cauldron': c.get('id'),
                        'type': 'ticket_mismatch',
                        'severity': 'medium',
                        'details': 'Ticket discrepancy detected'
                    })
                
                # Check for rapid fill (time to full < 10 minutes)
                if c.get('time_to_full_min') and c.get('time_to_full_min') < 10:
                    anomalies.append({
                        'cauldron': c.get('id'),
                        'type': 'rapid_fill',
                        'severity': 'high',
                        'details': f"Will overflow in {c.get('time_to_full_min')} minutes"
                    })
            
            return {'anomalies': anomalies, 'count': len(anomalies)}
        except Exception as e:
            return {'error': str(e)}
    
    def _analyze_trends(self):
        """Tool: Analyze cauldron fill trends over time"""
        try:
            status_data = self._get_status()
            if isinstance(status_data, dict) and 'error' in status_data:
                return status_data
            
            trends = []
            for c in status_data:
                pct = c.get('percent_full', 0)
                rate = c.get('fill_rate_per_min', 0)
                
                # Calculate trend direction
                if pct > 80:
                    trend = 'critical' if rate > 0 else 'stable'
                elif pct > 50:
                    trend = 'rising' if rate > 0.5 else 'moderate'
                else:
                    trend = 'healthy'
                
                trends.append({
                    'cauldron': c.get('id'),
                    'name': c.get('name'),
                    'current_level': pct,
                    'fill_rate': rate,
                    'trend': trend,
                    'time_to_full': c.get('time_to_full_min')
                })
            
            return {'trends': trends, 'total': len(trends)}
        except Exception as e:
            return {'error': str(e)}
    
    def _suggest_actions(self):
        """Tool: Proactively suggest actions based on current state"""
        try:
            status_data = self._get_status()
            anomalies = self._detect_anomalies()
            
            if isinstance(status_data, dict) and 'error' in status_data:
                return status_data
            
            suggestions = []
            
            # Check for cauldrons needing immediate attention
            for c in status_data:
                cid = c.get('id')
                pct = c.get('percent_full', 0)
                ttf = c.get('time_to_full_min')
                
                if pct > 95:
                    suggestions.append({
                        'priority': 'URGENT',
                        'cauldron': cid,
                        'action': 'dispatch_courier',
                        'reason': f'{c.get("name")} is {pct:.1f}% full - overflow imminent',
                        'eta_minutes': ttf
                    })
                elif pct > 85 and ttf and ttf < 15:
                    suggestions.append({
                        'priority': 'HIGH',
                        'cauldron': cid,
                        'action': 'schedule_pickup',
                        'reason': f'{c.get("name")} will be full in {ttf:.0f} minutes',
                        'eta_minutes': ttf
                    })
                elif pct < 20 and c.get('fill_rate_per_min', 0) < 0:
                    suggestions.append({
                        'priority': 'LOW',
                        'cauldron': cid,
                        'action': 'investigate_drain',
                        'reason': f'{c.get("name")} is draining unexpectedly',
                        'eta_minutes': None
                    })
            
            # Check for optimization opportunities
            if len([s for s in suggestions if s['priority'] in ['URGENT', 'HIGH']]) >= 3:
                suggestions.append({
                    'priority': 'MEDIUM',
                    'cauldron': 'MULTIPLE',
                    'action': 'optimize_routes',
                    'reason': 'Multiple cauldrons need attention - route optimization recommended',
                    'eta_minutes': None
                })
            
            # Check for ticket discrepancies
            if anomalies.get('count', 0) > 5:
                suggestions.append({
                    'priority': 'MEDIUM',
                    'cauldron': 'SYSTEM',
                    'action': 'audit_tickets',
                    'reason': f'{anomalies["count"]} anomalies detected - ticket audit recommended',
                    'eta_minutes': None
                })
            
            return {'suggestions': suggestions, 'count': len(suggestions)}
        except Exception as e:
            return {'error': str(e)}
    
    def _compare_performance(self):
        """Tool: Compare current performance to historical averages"""
        try:
            status_data = self._get_status()
            if isinstance(status_data, dict) and 'error' in status_data:
                return status_data
            
            # Calculate system-wide metrics
            total_capacity = sum(c.get('max_volume', 0) for c in factory_static_data.get('cauldrons', []))
            current_volume = sum(c.get('current_level', 0) for c in status_data)
            avg_fill_pct = (current_volume / total_capacity * 100) if total_capacity > 0 else 0
            
            high_risk = len([c for c in status_data if c.get('percent_full', 0) > 85])
            medium_risk = len([c for c in status_data if 50 < c.get('percent_full', 0) <= 85])
            low_risk = len([c for c in status_data if c.get('percent_full', 0) <= 50])
            
            return {
                'system_utilization': round(avg_fill_pct, 1),
                'total_capacity': total_capacity,
                'current_volume': round(current_volume, 1),
                'risk_distribution': {
                    'high': high_risk,
                    'medium': medium_risk,
                    'low': low_risk
                },
                'total_cauldrons': len(status_data),
                'performance_status': 'GOOD' if avg_fill_pct < 70 else 'WARNING' if avg_fill_pct < 85 else 'CRITICAL'
            }
        except Exception as e:
            return {'error': str(e)}
    
    def get_proactive_insights(self):
        """
        NEW: Proactive monitoring - generates insights without user prompt
        Returns important information the user should know about
        """
        insights = []
        
        try:
            # Get current state
            status = self._get_status()
            anomalies = self._detect_anomalies()
            suggestions = self._suggest_actions()
            performance = self._compare_performance()
            
            # Generate urgent alerts
            if isinstance(anomalies, dict) and anomalies.get('count', 0) > 0:
                for a in anomalies.get('anomalies', [])[:3]:
                    if a['severity'] == 'high':
                        insights.append({
                            'type': 'ALERT',
                            'severity': 'HIGH',
                            'message': f"⚠️ {a['cauldron']}: {a['details']}",
                            'timestamp': datetime.now().isoformat()
                        })
            
            # Performance warnings
            if isinstance(performance, dict):
                perf_status = performance.get('performance_status')
                if perf_status == 'CRITICAL':
                    insights.append({
                        'type': 'WARNING',
                        'severity': 'HIGH',
                        'message': f"🔴 System utilization at {performance['system_utilization']}% - critical level",
                        'timestamp': datetime.now().isoformat()
                    })
                elif perf_status == 'WARNING':
                    insights.append({
                        'type': 'INFO',
                        'severity': 'MEDIUM',
                        'message': f"🟡 System utilization at {performance['system_utilization']}% - monitor closely",
                        'timestamp': datetime.now().isoformat()
                    })
            
            # Action recommendations
            if isinstance(suggestions, dict) and suggestions.get('count', 0) > 0:
                urgent = [s for s in suggestions['suggestions'] if s['priority'] == 'URGENT']
                if urgent:
                    for sug in urgent[:2]:
                        insights.append({
                            'type': 'ACTION',
                            'severity': 'URGENT',
                            'message': f"🚨 {sug['reason']} - Recommended: {sug['action']}",
                            'timestamp': datetime.now().isoformat(),
                            'action': sug['action'],
                            'cauldron': sug['cauldron']
                        })
            
            return {
                'insights': insights,
                'count': len(insights),
                'generated_at': datetime.now().isoformat()
            }
            
        except Exception as e:
            return {'error': str(e), 'insights': [], 'count': 0}
    
    def plan_and_execute(self, user_message):
        """
        Multi-step workflow:
        1. Analyze user intent
        2. Create execution plan
        3. Execute tools in sequence
        4. Synthesize response
        """
        steps = []
        tool_results = {}
        
        # Step 1: Intent analysis
        intent = self._analyze_intent(user_message)
        steps.append(f"Intent Analysis: {intent['description']}")
        
        # Step 2: Create plan
        plan = self._create_plan(intent, user_message)
        steps.append(f"Execution Plan: {' → '.join(plan['steps'])}")
        
        # Step 3: Execute tools
        for tool_name in plan['tools']:
            if tool_name in self.tools:
                steps.append(f"Executing: {tool_name}")
                result = self._execute_tool(tool_name, plan.get('params', {}))
                tool_results[tool_name] = result
                steps.append(f"Result: {self._summarize_result(result)}")
        
        # Step 4: Synthesize response
        response = self._synthesize_response(user_message, intent, tool_results, steps)
        
        return {
            'steps': steps,
            'response': response,
            'intent': intent,
            'tool_results': tool_results
        }
    
    def _analyze_intent(self, message):
        """Analyze what the user wants to do"""
        msg_lower = message.lower().strip()
        
        # Check for confirmation responses (yes/no/ok)
        if msg_lower in ['yes', 'y', 'ok', 'okay', 'sure', 'do it', 'proceed', 'confirm', 'go ahead']:
            if self.pending_action:
                return {
                    'type': 'confirm_action', 
                    'description': f'Confirm pending action: {self.pending_action.get("action")}'
                }
        
        if msg_lower in ['no', 'n', 'cancel', 'stop', 'abort', 'nope']:
            self.pending_action = None
            return {'type': 'cancel', 'description': 'Cancel pending action'}
        
        # Regular intent analysis
        # CHECK DISPATCH FIRST - before predict (to catch "dispatch to all full cauldrons")
        if any(word in msg_lower for word in ['dispatch', 'send', 'empty', 'courier', 'drain']):
            # Check if it's a bulk dispatch request
            if any(word in msg_lower for word in ['all', 'multiple', 'every', 'bulk', '50%', 'above', 'over', 'threshold', 'half full', 'at least']):
                return {'type': 'action_bulk', 'description': 'Dispatch couriers to multiple cauldrons'}
            else:
                return {'type': 'action', 'description': 'Dispatch courier to manage cauldron'}
        elif any(word in msg_lower for word in ['suspicious', 'anomaly', 'ticket', 'discrepancy', 'problem', 'alert', 'issue']):
            return {'type': 'investigate', 'description': 'Investigate discrepancies and anomalies'}
        elif any(word in msg_lower for word in ['forecast', 'overflow', 'time', 'when', 'predict']):
            return {'type': 'predict', 'description': 'Forecast fill times and overflow risks'}
        elif any(word in msg_lower for word in ['optimize', 'route', 'witch', 'efficient']):
            return {'type': 'optimize', 'description': 'Optimize courier routes for efficiency'}
        elif any(word in msg_lower for word in ['status', 'how', 'level', 'current', 'what']):
            return {'type': 'monitor', 'description': 'Monitor current cauldron status'}
        elif any(word in msg_lower for word in ['network', 'map', 'topology', 'connection']):
            return {'type': 'analyze', 'description': 'Analyze network topology'}
        elif any(word in msg_lower for word in ['trend', 'pattern', 'history', 'over time']):
            return {'type': 'trends', 'description': 'Analyze trends and patterns'}
        elif any(word in msg_lower for word in ['suggest', 'recommend', 'what should', 'advice']):
            return {'type': 'suggest', 'description': 'Provide recommendations and suggestions'}
        elif any(word in msg_lower for word in ['compare', 'performance', 'metric', 'efficiency']):
            return {'type': 'performance', 'description': 'Compare performance metrics'}
        else:
            return {'type': 'general', 'description': 'General inquiry about factory'}
    
    def _create_plan(self, intent, message):
        """Create execution plan based on intent"""
        intent_type = intent['type']
        
        # Handle confirmation of pending action
        if intent_type == 'confirm_action' and self.pending_action:
            action_type = self.pending_action.get('action')
            cauldron_id = self.pending_action.get('cauldron_id')
            
            if action_type == 'dispatch_courier':
                return {
                    'steps': ['Execute confirmed dispatch', 'Update status'],
                    'tools': ['dispatch_courier'],
                    'params': {'cauldron_id': cauldron_id}
                }
            elif action_type == 'optimize_routes':
                return {
                    'steps': ['Execute confirmed optimization'],
                    'tools': ['optimize_routes']
                }
        
        # Handle cancellation
        if intent_type == 'cancel':
            return {
                'steps': ['Cancel pending action'],
                'tools': []
            }
        
        if intent_type == 'investigate':
            return {
                'steps': ['Check tickets', 'Detect anomalies', 'Get status', 'Generate report'],
                'tools': ['check_tickets', 'detect_anomalies', 'get_status']
            }
        elif intent_type == 'predict':
            return {
                'steps': ['Get forecasts', 'Detect anomalies', 'Prioritize risks'],
                'tools': ['forecast_fills', 'detect_anomalies']
            }
        elif intent_type == 'action_bulk':
            # Extract threshold from message if mentioned
            import re
            threshold = 50  # default
            match = re.search(r'(\d+)\s*%', message)
            if match:
                threshold = int(match.group(1))
            return {
                'steps': ['Get current status', 'Dispatch to all above threshold', 'Report results'],
                'tools': ['get_status', 'dispatch_bulk'],
                'params': {'threshold': threshold}
            }
        elif intent_type == 'action':
            # Extract cauldron ID from message
            cauldron_id = self._extract_cauldron_id(message)
            return {
                'steps': ['Verify target', 'Dispatch courier', 'Confirm action'],
                'tools': ['dispatch_courier'],
                'params': {'cauldron_id': cauldron_id}
            }
        elif intent_type == 'optimize':
            return {
                'steps': ['Analyze network', 'Get forecasts', 'Optimize routes', 'Validate plan'],
                'tools': ['analyze_network', 'forecast_fills', 'optimize_routes']
            }
        elif intent_type == 'monitor':
            return {
                'steps': ['Get status', 'Detect anomalies', 'Summarize findings'],
                'tools': ['get_status', 'detect_anomalies']
            }
        elif intent_type == 'analyze':
            return {
                'steps': ['Analyze network', 'Get status', 'Compute metrics'],
                'tools': ['analyze_network', 'get_status']
            }
        elif intent_type == 'trends':
            return {
                'steps': ['Analyze trends', 'Get status', 'Identify patterns'],
                'tools': ['analyze_trends', 'get_status']
            }
        elif intent_type == 'suggest':
            return {
                'steps': ['Suggest actions', 'Get status', 'Prioritize recommendations'],
                'tools': ['suggest_actions', 'get_status', 'detect_anomalies']
            }
        elif intent_type == 'performance':
            return {
                'steps': ['Compare performance', 'Analyze trends', 'Benchmark metrics'],
                'tools': ['compare_performance', 'analyze_trends']
            }
        else:
            return {
                'steps': ['Get status', 'Provide overview', 'Suggest next steps'],
                'tools': ['get_status', 'suggest_actions']
            }
    
    def _extract_cauldron_id(self, message):
        """Extract cauldron ID from user message - uses exact match priority"""
        import re
        msg_lower = message.lower()
        
        # First try exact cauldron_XXX pattern match (e.g., "cauldron_009")
        pattern = r'cauldron_(\d+)'
        match = re.search(pattern, msg_lower)
        if match:
            # Construct the full ID and verify it exists
            extracted_id = f"cauldron_{match.group(1)}"
            for cauldron in factory_static_data.get('cauldrons', []):
                if cauldron.get('id', '').lower() == extracted_id:
                    return cauldron.get('id')
        
        # Fallback: check by name or partial ID match (but prefer longer matches)
        best_match = None
        best_match_len = 0
        
        for cauldron in factory_static_data.get('cauldrons', []):
            cid = cauldron.get('id', '')
            name = cauldron.get('name', '')
            
            # Check if cauldron ID appears in message
            if cid.lower() in msg_lower:
                if len(cid) > best_match_len:
                    best_match = cid
                    best_match_len = len(cid)
            
            # Check if any word from cauldron name appears
            elif any(word in msg_lower for word in name.lower().split()):
                if len(name) > best_match_len:
                    best_match = cid
                    best_match_len = len(name)
        
        return best_match
    
    def _execute_tool(self, tool_name, params):
        """Execute a tool with parameters"""
        tool_func = self.tools.get(tool_name)
        if not tool_func:
            return {'error': f'Unknown tool: {tool_name}'}
        
        try:
            # Handle tools with parameters
            if tool_name == 'dispatch_courier' and 'cauldron_id' in params:
                return tool_func(params['cauldron_id'])
            elif tool_name == 'dispatch_bulk' and 'threshold' in params:
                return tool_func(params['threshold'])
            else:
                return tool_func()
        except Exception as e:
            return {'error': str(e)}
    
    def _summarize_result(self, result):
        """Create brief summary of tool result"""
        if isinstance(result, dict):
            if 'error' in result:
                return f"Error: {result['error']}"
            elif 'total_dispatched' in result:
                return f"Dispatched {result['total_dispatched']} couriers, {result['total_already_draining']} already draining"
            elif 'matches' in result:
                suspicious = len([m for m in result['matches'] if m.get('suspicious')])
                return f"Found {suspicious} suspicious tickets"
            elif 'anomalies' in result:
                return f"Detected {result['count']} anomalies"
            elif 'required_couriers' in result:
                return f"Need {result['required_couriers']} couriers"
            else:
                return "Success"
        elif isinstance(result, list):
            return f"{len(result)} items retrieved"
        return "Complete"
    
    def _synthesize_response(self, user_message, intent, tool_results, steps):
        """Generate final response using Nemotron or fallback"""
        if self.client:
            response = self._nemotron_synthesis(user_message, intent, tool_results, steps)
            # If Nemotron returns None or empty, use fallback
            if response and response.strip():
                return response
            print("[Nemotron] Empty response, using fallback synthesis")
        return self._fallback_synthesis(intent, tool_results)
    
    def _nemotron_synthesis(self, user_message, intent, tool_results, steps):
        """Use Nemotron to synthesize natural language response"""
        try:
            # Build context from tool results
            context_parts = [f"User Intent: {intent['description']}"]
            for tool_name, result in tool_results.items():
                context_parts.append(f"{tool_name}: {json.dumps(result, indent=2)}")
            
            context = "\n".join(context_parts)
            
            system_msg = (
                "You are an intelligent factory monitoring agent with access to real-time data and tools. "
                "Based on the tool execution results, provide a clear, actionable response to the user. "
                "Be concise but informative. Highlight any urgent issues and recommend next steps."
            )
            
            prompt = f"{context}\n\nUser Question: {user_message}\n\nProvide a helpful response:"
            
            messages = [
                {"role": "system", "content": system_msg},
                {"role": "user", "content": prompt}
            ]
            
            completion = self.client.chat.completions.create(
                model="nvidia/nvidia-nemotron-nano-9b-v2",
                messages=messages,
                temperature=0.6,
                max_tokens=512,
                stream=False
            )
            
            if hasattr(completion.choices[0], 'message') and completion.choices[0].message:
                content = completion.choices[0].message.content
                if content and content.strip():
                    return content
                print("[Nemotron] Empty content from message")
                return None
            print("[Nemotron] No message attribute in completion")
            return None
            
        except Exception as e:
            print(f"[Nemotron synthesis error]: {e}")
            return None  # Return None so _synthesize_response can use fallback
    
    def _fallback_synthesis(self, intent, tool_results):
        """Fallback response generation without Nemotron"""
        intent_type = intent['type']
        
        # Handle confirmation
        if intent_type == 'confirm_action':
            dispatch = tool_results.get('dispatch_courier', {})
            if dispatch.get('status') == 'success':
                self.pending_action = None  # Clear pending action
                
                # Check if already empty
                if dispatch.get('already_empty'):
                    return f"ℹ️ **{dispatch.get('cauldron_name', 'Cauldron')} is already empty!**\n\nNo courier dispatch needed - the cauldron is at 0% capacity."
                
                est_min = dispatch.get('estimated_minutes', 0)
                drain_rate = dispatch.get('drain_rate', 0)
                current = dispatch.get('current_level', 0)
                percent = dispatch.get('percent_full', 0)
                max_vol = dispatch.get('max_volume', 1)
                
                response = f"✅ **Courier Dispatched Successfully!**\n\n"
                response += f"• **Target:** {dispatch.get('cauldron_name', 'Unknown')}\n"
                response += f"• **Current Level:** {current:.1f}L / {max_vol}L ({percent:.1f}% full)\n"
                response += f"• **Drain Rate:** {drain_rate:.1f}L/min\n"
                response += f"• **Est. Completion:** ~{est_min} minutes\n"
                response += f"• **Dispatched:** {dispatch.get('dispatched_at', 'Now')}\n\n"
                response += f"🚛 _The courier is now actively draining the cauldron. Watch the dashboard for real-time progress!_"
                return response
            else:
                return f"⚠️ Dispatch failed: {dispatch.get('error', 'Unknown error')}"
        
        # Handle cancellation
        if intent_type == 'cancel':
            self.pending_action = None
            return "✓ Action canceled. Let me know if you need anything else."
        
        if intent_type == 'investigate':
            tickets = tool_results.get('check_tickets', {})
            anomalies = tool_results.get('detect_anomalies', {})
            
            response = "**Investigation Complete:**\n\n"
            if 'matches' in tickets:
                suspicious = [m for m in tickets['matches'] if m.get('suspicious')]
                response += f"• Found **{len(suspicious)} suspicious tickets**\n"
            if 'anomalies' in anomalies:
                response += f"• Detected **{anomalies['count']} anomalies**\n"
                for a in anomalies['anomalies'][:3]:
                    response += f"  - {a['cauldron']}: {a['details']} (severity: {a['severity']})\n"
            
            return response
        
        elif intent_type == 'action_bulk':
            bulk = tool_results.get('dispatch_bulk', {})
            if bulk.get('status') == 'success':
                dispatched = bulk.get('total_dispatched', 0)
                draining = bulk.get('total_already_draining', 0)
                empty = bulk.get('total_already_empty', 0)
                threshold = bulk.get('threshold', 50)
                
                response = f"✅ **Bulk Courier Dispatch Complete!**\n\n"
                response += f"• **Threshold:** {threshold}% full or more\n"
                response += f"• **New Dispatches:** {dispatched} courier(s) sent\n"
                response += f"• **Already Draining:** {draining} cauldron(s)\n"
                response += f"• **Already Empty:** {empty} cauldron(s)\n\n"
                
                if bulk.get('dispatched'):
                    response += "**Newly Dispatched:**\n"
                    for d in bulk['dispatched'][:5]:
                        response += f"  - {d['cauldron_name']}: {d['percent_full']:.1f}% → ~{d['estimated_minutes']:.0f} min\n"
                
                if bulk.get('already_draining'):
                    response += "\n**Already Draining:**\n"
                    for d in bulk['already_draining'][:3]:
                        response += f"  - {d['cauldron_name']}: {d['progress']:.1f}% complete\n"
                
                return response
            else:
                return f"⚠️ Bulk dispatch failed: {bulk.get('error', 'Unknown error')}"
        
        elif intent_type == 'predict':
            forecasts = tool_results.get('forecast_fills', [])
            if isinstance(forecasts, list) and len(forecasts) > 0:
                urgent = [f for f in forecasts if f.get('time_to_full_min', 9999) < 30]
                response = f"**Forecast Analysis:**\n\n• {len(urgent)} cauldrons will overflow within 30 minutes\n"
                for f in urgent[:5]:
                    response += f"  - {f.get('name')}: {f.get('time_to_full_min')} min\n"
                return response
            return "✓ No urgent overflow risks detected"
        
        elif intent_type == 'action':
            dispatch = tool_results.get('dispatch_courier', {})
            if dispatch.get('status') == 'success':
                self.pending_action = None  # Clear any pending action
                
                # Check if already empty
                if dispatch.get('already_empty'):
                    return f"ℹ️ **{dispatch.get('cauldron_name', 'Cauldron')} is already empty!**\n\nNo courier dispatch needed - the cauldron is at 0% capacity."
                
                # Check if already draining
                if dispatch.get('already_draining'):
                    progress = dispatch.get('drain_progress', 0)
                    elapsed = dispatch.get('elapsed_minutes', 0)
                    current = dispatch.get('current_level', 0)
                    initial = dispatch.get('initial_level', 0)
                    drain_rate = dispatch.get('drain_rate', 0)
                    
                    response = f"🚛 **Courier Already Draining {dispatch.get('cauldron_name', 'Cauldron')}!**\n\n"
                    response += f"• **Progress:** {progress:.1f}% drained\n"
                    response += f"• **Remaining:** {current:.1f}L (started at {initial:.1f}L)\n"
                    response += f"• **Time Elapsed:** {elapsed:.1f} minutes\n"
                    response += f"• **Drain Rate:** {drain_rate:.1f}L/min\n\n"
                    response += f"_The drain is ongoing. Check the dashboard for real-time updates!_"
                    return response
                
                est_min = dispatch.get('estimated_minutes', 0)
                drain_rate = dispatch.get('drain_rate', 0)
                current = dispatch.get('current_level', 0)
                percent = dispatch.get('percent_full', 0)
                max_vol = dispatch.get('max_volume', 1)
                
                response = f"✅ **Courier Dispatched Successfully!**\n\n"
                response += f"• **Target:** {dispatch.get('cauldron_name', 'Unknown')}\n"
                response += f"• **Current Level:** {current:.1f}L / {max_vol}L ({percent:.1f}% full)\n"
                response += f"• **Drain Rate:** {drain_rate:.1f}L/min\n"
                response += f"• **Est. Completion:** ~{est_min:.1f} minutes\n"
                response += f"• **Dispatched:** {dispatch.get('dispatched_at', 'Now')}\n\n"
                response += f"🚛 _The courier is now actively draining the cauldron. Watch the dashboard for real-time progress!_"
                return response
            elif 'error' in dispatch:
                return f"⚠️ **Dispatch Failed**\n\nError: {dispatch.get('error')}\n\n**Troubleshooting:**\n1. Verify cauldron ID is correct\n2. Check system connectivity\n3. Retry the operation\n4. If issue persists, contact support"
            return "Dispatch operation completed"
        
        elif intent_type == 'optimize':
            routes = tool_results.get('optimize_routes', {})
            if 'required_couriers' in routes:
                return f"**Route Optimization Complete:**\n\n• {routes['required_couriers']} couriers required\n• {len(routes.get('routes', []))} optimized routes computed\n• Ready for deployment"
            return "Route optimization complete"
        
        elif intent_type == 'suggest':
            suggestions = tool_results.get('suggest_actions', {})
            if suggestions.get('count', 0) > 0:
                response = f"**Recommendations ({suggestions['count']} total):**\n\n"
                urgent = [s for s in suggestions.get('suggestions', []) if s['priority'] == 'URGENT']
                high = [s for s in suggestions.get('suggestions', []) if s['priority'] == 'HIGH']
                
                if urgent:
                    response += "🚨 **URGENT:**\n"
                    for s in urgent[:3]:
                        response += f"  - {s['reason']}\n"
                        if s['action'] == 'dispatch_courier' and s['cauldron']:
                            response += f"    → Would you like me to dispatch a courier to **{s['cauldron']}** now?\n"
                            # Set pending action
                            self.pending_action = {
                                'action': 'dispatch_courier',
                                'cauldron_id': s['cauldron']
                            }
                
                if high:
                    response += "\n⚠️ **HIGH PRIORITY:**\n"
                    for s in high[:3]:
                        response += f"  - {s['reason']}\n"
                
                if self.pending_action:
                    response += "\n💬 _Reply 'yes' to confirm or 'no' to cancel._"
                
                return response
            return "✓ No urgent actions needed at this time"
        
        elif intent_type == 'performance':
            perf = tool_results.get('compare_performance', {})
            if perf:
                status = perf.get('performance_status', 'UNKNOWN')
                icon = '🔴' if status == 'CRITICAL' else '🟡' if status == 'WARNING' else '🟢'
                response = f"{icon} **System Performance: {status}**\n\n"
                response += f"• Overall Utilization: {perf.get('system_utilization', 0)}%\n"
                response += f"• Total Capacity: {perf.get('total_capacity', 0)}L\n"
                response += f"• Current Volume: {perf.get('current_volume', 0)}L\n"
                risk = perf.get('risk_distribution', {})
                response += f"\n**Risk Distribution:**\n"
                response += f"  - High Risk: {risk.get('high', 0)} cauldrons\n"
                response += f"  - Medium Risk: {risk.get('medium', 0)} cauldrons\n"
                response += f"  - Low Risk: {risk.get('low', 0)} cauldrons\n"
                return response
            return "Performance metrics retrieved"
        
        elif intent_type == 'trends':
            trends = tool_results.get('analyze_trends', {})
            if trends.get('total', 0) > 0:
                critical = [t for t in trends.get('trends', []) if t['trend'] == 'critical']
                rising = [t for t in trends.get('trends', []) if t['trend'] == 'rising']
                response = f"**Trend Analysis ({trends['total']} cauldrons):**\n\n"
                if critical:
                    response += f"🔴 **Critical Trends:** {len(critical)} cauldrons\n"
                    for t in critical[:3]:
                        response += f"  - {t['name']}: {t['current_level']:.1f}% ({t['trend']})\n"
                if rising:
                    response += f"\n🟡 **Rising Trends:** {len(rising)} cauldrons\n"
                return response
            return "Trend analysis complete"
        
        else:
            # General or default response
            status = tool_results.get('get_status', [])
            suggestions = tool_results.get('suggest_actions', {})
            
            if isinstance(status, list):
                total = len(status)
                critical = len([c for c in status if c.get('percent_full', 0) > 90])
                draining = [c for c in status if c.get('is_draining')]
                
                response = f"**Factory Status:**\n\n• {total} cauldrons monitored\n• {critical} at critical fill levels (>90%)\n"
                
                # Show draining cauldrons
                if draining:
                    response += f"• 🚛 {len(draining)} courier{'s' if len(draining) > 1 else ''} actively draining\n"
                    for c in draining[:2]:
                        progress = c.get('drain_progress', 0)
                        response += f"  - **{c.get('name')}**: {progress:.1f}% drained (current: {c.get('current_level', 0):.1f}L)\n"
                
                # Add urgent suggestions if available
                if suggestions.get('count', 0) > 0:
                    urgent = [s for s in suggestions.get('suggestions', []) if s['priority'] == 'URGENT']
                    if urgent:
                        response += f"\n🚨 **{len(urgent)} urgent issue{'s' if len(urgent) > 1 else ''}:**\n"
                        for s in urgent[:2]:
                            response += f"  - {s['reason']}\n"
                            if s['action'] == 'dispatch_courier' and s['cauldron']:
                                # Set pending action for first urgent item
                                if not self.pending_action:
                                    self.pending_action = {
                                        'action': 'dispatch_courier',
                                        'cauldron_id': s['cauldron']
                                    }
                                    response += f"\n💬 Would you like me to dispatch a courier to **{s['cauldron']}** now? (Reply 'yes' or 'no')"
                                    break
                
                if not self.pending_action:
                    response += "\n\n_Ask me for suggestions, trends, or specific actions!_"
                
                return response
            return "Status check complete"

# --- Setup ---
app = Flask(__name__)
CORS(app) 

# Session configuration. If SECRET_KEY is not set, use an ephemeral key for
# local development (but warn loudly).
app.secret_key = os.environ.get("SECRET_KEY")
if not app.secret_key:
    print("[auth] WARNING: SECRET_KEY not set; using ephemeral key for dev. Set SECRET_KEY in env for production.")
    app.secret_key = os.urandom(24)

# If authlib is available and Auth0 env vars exist, register OAuth. Otherwise
# provide no-op/dummy routes so the app remains runnable in development.
if _HAVE_AUTHLIB and os.environ.get("AUTH0_DOMAIN"):
    oauth = OAuth(app)
    oauth.register(
        "auth0",
        client_id=os.environ.get("AUTH0_CLIENT_ID"),
        client_secret=os.environ.get("AUTH0_CLIENT_SECRET"),
        client_kwargs={
            "scope": "openid profile email",
        },
        server_metadata_url=f'https://{os.environ.get("AUTH0_DOMAIN")}/.well-known/openid-configuration'
    )

    def requires_auth(f):
        @wraps(f)
        def decorated(*args, **kwargs):
            if 'user' not in session:
                return redirect(url_for('login'))
            return f(*args, **kwargs)
        return decorated

    @app.route('/login')
    def login():
        # Allow an explicit override for the redirect URI to make local
        # development and tunneling (ngrok) easier. If not provided, fall
        # back to the URL generated by Flask's `url_for`.
        redirect_uri = os.environ.get('AUTH_CALLBACK_URL') or url_for("callback", _external=True)
        print(f"[auth] Redirecting to Auth0 authorize endpoint with redirect_uri={redirect_uri}")
        return oauth.auth0.authorize_redirect(redirect_uri=redirect_uri)

    @app.route('/callback')
    def callback():
        try:
            token = oauth.auth0.authorize_access_token()
            session["user"] = token
            return redirect(url_for("loading"))
        except Exception as e:
            print(f"Auth error: {e}")
            return redirect(url_for("index"))

    @app.route('/logout')
    def logout():
        session.clear()
        return redirect(
            "https://" + os.environ.get("AUTH0_DOMAIN")
            + "/v2/logout?"
            + urlencode(
                {
                    "returnTo": url_for("index", _external=True),
                    "client_id": os.environ.get("AUTH0_CLIENT_ID"),
                },
                quote_via=quote_plus,
            )
        )
else:
    # Auth not available: create a passthrough requires_auth so protected
    # endpoints remain accessible during local development. Also expose
    # informative login/logout endpoints.
    def requires_auth(f):
        @wraps(f)
        def decorated(*args, **kwargs):
            # No-op in dev when authlib/Auth0 not configured
            return f(*args, **kwargs)
        return decorated

    @app.route('/login')
    def login_not_configured():
        # Informative message and guidance for devs
        msg = {
            "error": "Auth not configured. Install authlib and set AUTH0_DOMAIN/AUTH0_CLIENT_ID/AUTH0_CLIENT_SECRET in env.",
            "hint": "If you want to test Auth0 locally, add the exact redirect URI (e.g. http://127.0.0.1:5000/callback) to the Auth0 Allowed Callback URLs. You can also set AUTH_CALLBACK_URL to force the redirect URI used by the app."
        }
        return jsonify(msg), 501

    @app.route('/callback')
    def callback_not_configured():
        return jsonify({"error": "Auth callback not available because auth is not configured."}), 501

    @app.route('/logout')
    def logout_not_configured():
        return jsonify({"error": "Logout not available because auth is not configured."}), 501
    
    # Development helper: allow a dev login when explicitly enabled via env.
    @app.route('/login/dev')
    def login_dev():
        """Simulate a login for local development when ALLOW_DEV_LOGIN=1 is set.
        This creates a fake session user and redirects to the dashboard.
        """
        if os.environ.get('ALLOW_DEV_LOGIN') == '1' or os.environ.get('FLASK_ENV') == 'development':
            session['user'] = {'sub': 'dev_user', 'email': 'dev@example.com', 'name': 'Developer'}
            return redirect(url_for('dashboard'))
        return jsonify({'error': 'Dev login not enabled. Set ALLOW_DEV_LOGIN=1 in environment.'}), 403

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
# Lightweight auth status endpoint (does not expose secrets)
@app.route('/auth/status')
def auth_status():
    return jsonify({
        'authlib_installed': True if 'oauth' in globals() else False,
        'auth0_domain_present': bool(os.environ.get('AUTH0_DOMAIN')),
        'auth0_client_id_present': bool(os.environ.get('AUTH0_CLIENT_ID')),
        'auth0_client_secret_present': bool(os.environ.get('AUTH0_CLIENT_SECRET')),
        'secret_key_present': bool(os.environ.get('SECRET_KEY'))
    })


@app.route('/api/user')
def api_user():
    """Return a lightweight user profile for the frontend (does not expose secrets).
    Returns 401 when no user is logged in.
    """
    user = session.get('user')
    if not user:
        return jsonify({'error': 'not authenticated'}), 401

    profile = {}
    try:
        # Common shapes: dev login stores a simple dict; authlib token may include 'userinfo'
        if isinstance(user, dict):
            if 'userinfo' in user and isinstance(user['userinfo'], dict):
                profile.update(user['userinfo'])
            # copy common top-level claims if present
            for k in ('name', 'email', 'picture', 'sub'):
                if k in user and user[k]:
                    profile.setdefault(k, user[k])
        # ensure minimal shape
        out = {
            'name': profile.get('name') or profile.get('nickname') or profile.get('email'),
            'email': profile.get('email'),
            'picture': profile.get('picture'),
            'sub': profile.get('sub')
        }
        return jsonify(out)
    except Exception as e:
        print('[auth] Failed to serialize user profile:', e)
        return jsonify({'error': 'internal error'}), 500
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
        raw = safe_get(EOG_API_BASE_URL + '/api/Data?start_date=0&end_date=2000000000', timeout=20) or []
    except Exception:
        return {}

    # API returns array directly according to documentation
    data_list = raw if isinstance(raw, list) else []
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
@requires_auth
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
    # Don't abruptly exit the process; instead continue with empty
    # placeholders so the Flask app can run and surface errors via
    # endpoints. Calling exit() here raised SystemExit in the
    # debugger (code 3 in some environments) and made local dev
    # iteration painful.
    print("[init] WARNING: Could not load static factory data from EOG API. Continuing with empty defaults.")
    factory_static_data = {
        "cauldrons": [],
        "network": {},
        "market": {},
        "couriers": []
    }

# Clear any active drains on startup (fresh start)
print("[init] Clearing all active drains (fresh app start)")
with drains_lock:
    active_drains.clear()
with resolved_tickets_lock:
    resolved_tickets.clear()

# Global set to track cauldrons with suspicious tickets (populated by /api/tickets/match)
suspicious_cauldrons = set()

# Server-side forecast smoothing state to avoid large upward jumps in full_at
forecast_state = {}

# Start background refresh of rates so we rely on real API-derived numbers where possible
try:
    _refresh_rates_periodically(interval_seconds=60)
except Exception:
    pass

# --- EOG Challenge: Tool Definitions (API Endpoints) ---

@app.route('/api/cauldron/levels')
@requires_auth
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
@requires_auth
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
@requires_auth
def cauldron_status():
    """
    Returns merged cauldron data including current level, percentage full,
    and estimated time to full (minutes) by calling existing tools.
    Frontend dashboard will poll this endpoint.
    """
    # Calculate request timestamp ONCE for this entire request
    # This prevents time-to-full from jumping around on every poll
    request_timestamp = datetime.utcnow().replace(tzinfo=timezone.utc)
    
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
        
        # Apply active drain if courier was dispatched
        cauldron_id = c.get('id')
        is_draining = False
        drain_progress = 0
        
        global active_drains, drains_lock
        
        with drains_lock:
            if cauldron_id in active_drains:
                drain_info = active_drains[cauldron_id]
                elapsed = (datetime.now() - drain_info['start_time']).total_seconds() / 60  # minutes
                drained_amount = elapsed * drain_info['drain_rate']
                
                # Calculate current level after draining
                new_level = max(0, drain_info['initial_level'] - drained_amount)
                
                if new_level <= 0:
                    # Drain complete, remove from active drains
                    print(f"[DRAIN] ✓ Complete for {drain_info['cauldron_name']} - drained {drained_amount:.1f}L in {elapsed:.1f} min")
                    del active_drains[cauldron_id]
                    current = 0
                else:
                    # Still draining
                    current = new_level
                    is_draining = True
                    drain_progress = (drained_amount / drain_info['initial_level']) * 100 if drain_info['initial_level'] > 0 else 100
                    
                    # Log every 10% milestone
                    if int(drain_progress) % 10 == 0 and int(drain_progress) > 0:
                        milestone = int(drain_progress // 10) * 10
                        if not hasattr(drain_info, f'logged_{milestone}'):
                            print(f"[DRAIN] {drain_info['cauldron_name']}: {drain_progress:.1f}% complete ({current:.1f}L remaining)")
                            setattr(drain_info, f'logged_{milestone}', True)
        
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
        status['current_level'] = current  # Override with drained level
        status['percent_full'] = percent
        status['time_to_full_min'] = time_to_full_min
        status['time_to_full_seconds'] = time_to_full_seconds
        status['has_discrepancy'] = c.get('id') in suspicious_cauldrons
        status['is_draining'] = is_draining
        status['drain_progress'] = round(drain_progress, 1) if is_draining else 0
        
        # Use the shared request_timestamp for all cauldrons in this response
        # This prevents time-to-full from jumping around
        try:
            status['as_of'] = request_timestamp.isoformat()
            if time_to_full_seconds is not None:
                try:
                    # compute proposed full_at using the shared request timestamp
                    final_full_at = request_timestamp + timedelta(seconds=int(time_to_full_seconds))
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


@app.route('/api/couriers/dispatch-bulk', methods=['POST'])
@requires_auth
def dispatch_couriers_bulk():
    """Dispatch couriers to multiple cauldrons at once based on fill threshold"""
    data = request.get_json() or {}
    threshold_percent = data.get('threshold_percent', 50)  # Default 50%
    
    try:
        # Get current cauldron status
        status_response = cauldron_status()
        if status_response.status_code != 200:
            return jsonify({'error': 'Could not fetch cauldron status'}), 500
        
        cauldrons = status_response.get_json()
        
        # Find cauldrons above threshold
        dispatched = []
        failed = []
        already_draining = []
        
        agent = AgentWorkflow()
        
        for c in cauldrons:
            cauldron_id = c.get('id')
            percent = c.get('percent_full', 0)
            is_draining = c.get('is_draining', False)
            
            if percent >= threshold_percent:
                if is_draining:
                    already_draining.append({
                        'id': cauldron_id,
                        'name': c.get('name'),
                        'percent': percent
                    })
                else:
                    # Dispatch courier
                    result = agent._dispatch_courier(cauldron_id)
                    if result.get('status') == 'success':
                        dispatched.append({
                            'id': cauldron_id,
                            'name': c.get('name'),
                            'percent': percent,
                            'current_level': c.get('current_level')
                        })
                    else:
                        failed.append({
                            'id': cauldron_id,
                            'name': c.get('name'),
                            'error': result.get('error', 'Unknown error')
                        })
        
        return jsonify({
            'threshold_percent': threshold_percent,
            'dispatched': dispatched,
            'already_draining': already_draining,
            'failed': failed,
            'summary': f"Dispatched {len(dispatched)} courier(s), {len(already_draining)} already draining, {len(failed)} failed"
        })
        
    except Exception as e:
        return jsonify({'error': str(e)}), 500


@app.route('/api/debug/drains')
@requires_auth
def debug_drains():
    """Debug endpoint to check active drain status"""
    global active_drains, drains_lock
    
    with drains_lock:
        debug_info = {}
        for cid, drain in active_drains.items():
            elapsed = (datetime.now() - drain['start_time']).total_seconds() / 60
            drained = elapsed * drain['drain_rate']
            remaining = max(0, drain['initial_level'] - drained)
            progress = (drained / drain['initial_level'] * 100) if drain['initial_level'] > 0 else 100
            
            debug_info[cid] = {
                'name': drain['cauldron_name'],
                'initial_level': drain['initial_level'],
                'current_level': remaining,
                'drain_rate': drain['drain_rate'],
                'elapsed_minutes': round(elapsed, 2),
                'progress_percent': round(progress, 2),
                'started_at': drain['start_time'].isoformat()
            }
    
    return jsonify({
        'active_drains': len(active_drains),
        'drains': debug_info
    })


@app.route('/api/data/historic')
@requires_auth
def data_historic():
    """Return historic /api/Data records filtered by query params:
    - start: ISO date (inclusive)
    - end: ISO date (inclusive)
    - cauldron_id: optional, filter to a single cauldron's level map
    """
    try:
        start_q = request.args.get('start')
        end_q = request.args.get('end')
        cauldron_id = request.args.get('cauldron_id')

        print(f"[HISTORIC] Fetching data for date range: {start_q} to {end_q}")

        raw = safe_get(EOG_API_BASE_URL + '/api/Data')
        if raw is None:
            print("[HISTORIC] Failed to fetch /api/Data")
            return jsonify({'error': 'Could not fetch /api/Data (timeout or API error)'}), 500

        # API returns array directly according to documentation
        data_list = raw if isinstance(raw, list) else []
        print(f"[HISTORIC] Received {len(data_list)} records from API")

        # Parse filter times
        start_dt = None
        end_dt = None
        if start_q:
            try:
                # Parse and make timezone-aware (assume UTC if no timezone specified)
                dt_str = start_q if 'T' in start_q else start_q + 'T00:00:00'
                start_dt = _parse_timestamp(dt_str)
                if start_dt and start_dt.tzinfo is None:
                    start_dt = start_dt.replace(tzinfo=timezone.utc)
                print(f"[HISTORIC] Parsed start time: {start_dt}")
            except Exception as e:
                print(f"[HISTORIC] Error parsing start time: {e}")
                import traceback
                traceback.print_exc()
                start_dt = None
        if end_q:
            try:
                # Parse and make timezone-aware (assume UTC if no timezone specified)
                dt_str = end_q if 'T' in end_q else end_q + 'T23:59:59'
                end_dt = _parse_timestamp(dt_str)
                if end_dt and end_dt.tzinfo is None:
                    end_dt = end_dt.replace(tzinfo=timezone.utc)
                print(f"[HISTORIC] Parsed end time: {end_dt}")
            except Exception as e:
                print(f"[HISTORIC] Error parsing end time: {e}")
                import traceback
                traceback.print_exc()
                end_dt = None

        out = []
        for rec in data_list:
            try:
                ts = None
                if isinstance(rec, dict):
                    ts = _parse_timestamp(rec.get('timestamp') or rec.get('time') or rec.get('t'))
                    # Make timezone-aware if needed
                    if ts and ts.tzinfo is None:
                        ts = ts.replace(tzinfo=timezone.utc)
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
            except Exception as e:
                print(f"[HISTORIC] Error processing record: {e}")
                continue

        print(f"[HISTORIC] Returning {len(out)} filtered records")
        return jsonify(out)
        
    except Exception as e:
        print(f"[HISTORIC] Error in data_historic endpoint: {str(e)}")
        import traceback
        traceback.print_exc()
        return jsonify({'error': f'Internal server error: {str(e)}'}), 500


@app.route('/api/debug/ticket-matching/<ticket_id>')
@requires_auth
def debug_ticket_matching(ticket_id):
    """Debug endpoint to inspect ticket matching details for a specific ticket."""
    try:
        # Fetch tickets
        tickets_raw = safe_get(EOG_API_BASE_URL + '/api/Tickets', timeout=5)
        if tickets_raw is None:
            return jsonify({'error': 'Could not fetch tickets'}), 500
        
        tickets_list = tickets_raw if isinstance(tickets_raw, list) else (tickets_raw.get('data') if isinstance(tickets_raw, dict) else [])
        
        # Find the ticket
        ticket = None
        for t in tickets_list:
            tid = t.get('id') or t.get('ticket_id') or t.get('ticketId')
            if str(tid) == str(ticket_id):
                ticket = t
                break
        
        if not ticket:
            return jsonify({'error': f'Ticket {ticket_id} not found'}), 404
        
        # Fetch data
        data_raw = safe_get(EOG_API_BASE_URL + '/api/Data', timeout=10)
        if data_raw is None:
            return jsonify({'error': 'Could not fetch historical data'}), 500
        
        # API returns array directly according to documentation
        data_list = data_raw if isinstance(data_raw, list) else []
        
        # Build series
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
        
        # Sort series
        for cid in series_map:
            series_map[cid].sort(key=lambda x: x[0])
        
        # Get ticket info
        cauldron_id = ticket.get('cauldronId') or ticket.get('cauldron_id') or ticket.get('cauldron')
        date_str = ticket.get('date') or ticket.get('day') or ticket.get('ticket_date')
        amount = _extract_ticket_amount(ticket)
        
        # Parse date
        match_day = None
        if date_str:
            try:
                if len(date_str) <= 10:
                    match_day = datetime.fromisoformat(date_str).date().isoformat()
                else:
                    dt = _parse_timestamp(date_str)
                    if dt:
                        match_day = dt.date().isoformat()
            except Exception:
                pass
        
        # Find drain events
        if not cauldron_id or cauldron_id not in series_map:
            return jsonify({
                'ticket': ticket,
                'error': 'Cauldron not found in historical data',
                'cauldron_id': cauldron_id
            })
        
        series = series_map[cauldron_id]
        static = next((c for c in factory_static_data['cauldrons'] if c['id'] == cauldron_id), None)
        fill_rate = static.get('fill_rate_per_min', 0) if static else 0
        
        # Find all drains for this cauldron
        all_drains = []
        i = 0
        n = len(series)
        while i < n-1:
            t0, v0 = series[i]
            j = i+1
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
                drained_adjusted = drained + (fill_rate * duration_min)
                
                all_drains.append({
                    'start': start_t.isoformat(),
                    'end': end_t.isoformat(),
                    'day': start_t.date().isoformat(),
                    'start_v': round(start_v, 2),
                    'end_v': round(end_v, 2),
                    'duration_min': round(duration_min, 1),
                    'drained': round(drained_adjusted, 2),
                    'fill_rate': fill_rate
                })
                i = j
            else:
                i += 1
        
        # Find matching drains (±1 day)
        matching_drains = []
        if match_day:
            try:
                match_date = datetime.fromisoformat(match_day).date()
                for drain in all_drains:
                    drain_date = datetime.fromisoformat(drain['day']).date()
                    if abs((drain_date - match_date).days) <= 1:
                        matching_drains.append(drain)
            except Exception:
                pass
        
        total_calculated = sum(d['drained'] for d in matching_drains) if matching_drains else None
        diff = None
        if amount is not None and total_calculated is not None:
            diff = round(amount - total_calculated, 2)
        
        return jsonify({
            'ticket': {
                'id': ticket_id,
                'cauldron_id': cauldron_id,
                'date': date_str,
                'match_day': match_day,
                'amount': amount
            },
            'fill_rate': fill_rate,
            'all_drains_count': len(all_drains),
            'all_drains': all_drains[:10],  # Limit to first 10 for readability
            'matching_drains_count': len(matching_drains),
            'matching_drains': matching_drains,
            'total_calculated': total_calculated,
            'difference': diff,
            'suspicious': abs(diff) > 10 and abs(diff) > 0.2 * max(1.0, amount) if diff is not None else None
        })
        
    except Exception as e:
        import traceback
        traceback.print_exc()
        return jsonify({'error': str(e)}), 500


@app.route('/api/network')
@requires_auth
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
@requires_auth
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

    # Deduplicate tickets by ticket_id (API may return duplicates)
    # AND filter out resolved/completed tickets (both from API and locally resolved)
    global resolved_tickets, resolved_tickets_lock
    
    seen_tickets = {}
    with resolved_tickets_lock:
        for t in tickets_list:
            ticket_id = t.get('id') or t.get('ticket_id') or t.get('ticketId')
            # Check if ticket is resolved/completed
            status = (t.get('status') or t.get('state') or t.get('resolved') or '').lower()
            is_resolved_api = status in ['resolved', 'completed', 'done', 'closed', 'finished'] or t.get('resolved') == True
            is_resolved_local = ticket_id in resolved_tickets
            
            # Only include unresolved tickets (not resolved via API or locally)
            if ticket_id and ticket_id not in seen_tickets and not is_resolved_api and not is_resolved_local:
                seen_tickets[ticket_id] = t
    
    tickets_list = list(seen_tickets.values())

    # Fetch full historical data once
    data_raw = safe_get(EOG_API_BASE_URL + '/api/Data')
    if data_raw is None:
        return jsonify({'error': 'Could not fetch /api/Data (timeout or API error)'}), 500

    # API returns array directly according to documentation
    data_list = data_raw if isinstance(data_raw, list) else []

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
        
        # More robust drain detection: look for net decrease over time windows
        # This handles cases where fill_rate causes small increases during draining
        i = 0
        n = len(series)
        while i < n-1:
            t0, v0 = series[i]
            j = i+1
            
            # Start of potential drain: level decreased
            if series[j][1] < v0:
                start_t = t0
                start_v = v0
                end_t = series[j][0]
                end_v = series[j][1]
                j += 1
                
                # Continue drain event even if there are small increases (fill_rate)
                # But stop when level stops decreasing (stabilizes or increases significantly)
                # Also limit drain duration AND amount to prevent merging multiple courier visits
                MAX_DRAIN_DURATION_MIN = 10  # Split drains longer than 10 minutes (was 15)
                MAX_DRAIN_AMOUNT_L = 110  # Split if drained more than 110L (was 120L, typical ticket is ~95L)
                consecutive_increases = 0
                consecutive_stable = 0
                while j < n:
                    prev_v = series[j-1][1]
                    curr_v = series[j][1]
                    curr_t = series[j][0]
                    
                    # Check if drain has been going on too long OR drained too much
                    duration_so_far = (curr_t - start_t).total_seconds() / 60.0
                    drained_so_far = start_v - curr_v
                    if duration_so_far > MAX_DRAIN_DURATION_MIN or drained_so_far > MAX_DRAIN_AMOUNT_L:
                        # Split here - this is likely a separate drain event
                        break
                    
                    # If actively decreasing, continue
                    if curr_v < prev_v - 0.5:  # Decreasing by >0.5L
                        end_t = curr_t
                        end_v = curr_v
                        consecutive_increases = 0
                        consecutive_stable = 0
                        j += 1
                    # Allow small increases (fill_rate) but limit them
                    elif curr_v > prev_v and consecutive_increases < 2:
                        consecutive_increases += 1
                        consecutive_stable = 0
                        j += 1
                    # Allow stable/near-stable points but limit them
                    elif abs(curr_v - prev_v) <= 0.5 and consecutive_stable < 3:
                        consecutive_stable += 1
                        j += 1
                    else:
                        # Drain has ended (stopped decreasing, or too many stable/increasing points)
                        break

                duration_min = (end_t - start_t).total_seconds() / 60.0
                drained = max(0.0, start_v - end_v)
                
                # Only record as drain if significant (>1L drained) to filter extreme noise
                if drained > 1:
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
        
        # Match drain events ONLY on the exact ticket date
        # If multiple drains exist on that day, find the one closest to the ticket amount
        if cauldron_id and match_day and cauldron_id in drains_by_cauldron_day:
            day_drains = drains_by_cauldron_day.get(cauldron_id, {}).get(match_day, [])
            if day_drains:
                if amount is not None:
                    # Find the drain closest to the ticket amount
                    best_drain = min(day_drains, key=lambda d: abs(d['drained'] - amount))
                    matched_events = [best_drain]
                    calculated = best_drain['drained']
                else:
                    # No ticket amount, just take the first drain
                    matched_events = [day_drains[0]]
                    calculated = day_drains[0]['drained']

        # If we couldn't compute from events, fallback to per-sample diff sum
        if calculated is None and cauldron_id:
            # try naive computation over series_map
            series = series_map.get(cauldron_id, [])
            # sum all decreases within the exact calendar day only
            if match_day:
                try:
                    s = 0.0
                    for i in range(len(series)-1):
                        a_ts, a_v = series[i]
                        b_ts, b_v = series[i+1]
                        # Check if both timestamps are on the exact match_day
                        if a_ts.date().isoformat() == match_day and b_ts.date().isoformat() == match_day:
                            if b_v < a_v:
                                s += (a_v - b_v)
                    calculated = s if s > 0 else None
                except Exception:
                    pass

        # Determine suspicious: use very lenient thresholds to minimize false positives
        suspicious = False
        diff = None
        reason = ''
        if amount is not None and calculated is not None:
            diff = round(amount - calculated, 2)
            # Very lenient threshold: Allow 50L difference OR 50% variance
            # (either condition alone makes it acceptable - only both together is suspicious)
            if abs(diff) > 50 and abs(diff) > 0.5 * max(1.0, amount):
                suspicious = True
                reason = f'Difference {diff}L exceeds both thresholds (>50L AND >50%).'
            else:
                reason = f'Match OK (diff: {diff}L)'
        elif amount is not None and calculated is None:
            # No drain event found - only suspicious if it's a very large amount
            if amount > 100:  # Only flag as suspicious if ticket is >100L (likely real fraud)
                suspicious = True
                reason = f'No matching drain event found for large {amount}L ticket.'
            else:
                suspicious = False
                reason = f'No drain found for {amount}L ticket (acceptable - may be timing issue).'
        elif amount is None:
            # Ticket has no amount - can't validate but not suspicious
            suspicious = False
            reason = 'Ticket has no amount data.'
        else:
            suspicious = False
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

    # Update global suspicious_cauldrons set
    global suspicious_cauldrons
    suspicious_cauldrons = {r['cauldron_id'] for r in results if r.get('suspicious')}

    return jsonify({'matches': results, 'unmatched_drains': unmatched_drains})

@app.route('/api/logistics/dispatch_courier', methods=['POST'])
@requires_auth
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
@requires_auth
def handle_agent_chat():
    """
    Enhanced multi-agent system demonstrating NVIDIA requirements:
    1. Beyond chatbot - implements complex workflows
    2. Multi-step - plan → execute → synthesize
    3. Tool integration - uses external APIs intelligently
    4. Real-world - solves actual factory monitoring
    """
    user_message = request.json.get('message', '')
    nv_api_key = request.json.get('nv_api_key') or _get_nv_api_key_from_env()
    use_nemotron = bool(request.json.get('use_nemotron')) or bool(nv_api_key)
    
    # Initialize Nemotron client if available
    nemotron_client = None
    if use_nemotron and _HAS_NEMOTRON and nv_api_key:
        try:
            nemotron_client = OpenAI(
                base_url="https://integrate.api.nvidia.com/v1",
                api_key=nv_api_key
            )
        except Exception as e:
            print(f"[Nemotron] Failed to initialize client: {e}")
    
    # Create agent workflow
    agent = AgentWorkflow(nemotron_client=nemotron_client)
    
    # Execute multi-step workflow
    result = agent.plan_and_execute(user_message)
    
    return jsonify({
        'agent_response': result['response'],
        'agent_plan': result['steps'],
        'intent': result['intent'],
        'workflow_complete': True
    })


@app.route('/api/agent/insights')
@requires_auth
def get_agent_insights():
    """
    NEW: Proactive monitoring endpoint
    Returns important insights without user prompting
    """
    nv_api_key = request.args.get('nv_api_key') or _get_nv_api_key_from_env()
    
    # Initialize Nemotron client if available
    nemotron_client = None
    if _HAS_NEMOTRON and nv_api_key:
        try:
            nemotron_client = OpenAI(
                base_url="https://integrate.api.nvidia.com/v1",
                api_key=nv_api_key
            )
        except Exception:
            pass
    
    # Create agent and get proactive insights
    agent = AgentWorkflow(nemotron_client=nemotron_client)
    insights = agent.get_proactive_insights()
    
    return jsonify(insights)


@app.route('/api/drains/reset', methods=['POST'])
@requires_auth
def reset_drains():
    """
    Manually reset all active drains.
    Useful for clearing stuck drains or testing.
    """
    global active_drains, resolved_tickets, drains_lock, resolved_tickets_lock
    
    try:
        with drains_lock:
            count = len(active_drains)
            active_drains.clear()
        
        with resolved_tickets_lock:
            ticket_count = len(resolved_tickets)
            resolved_tickets.clear()
        
        print(f"[RESET] Cleared {count} active drain(s) and {ticket_count} resolved ticket(s)")
        
        return jsonify({
            'status': 'success',
            'message': f'Cleared {count} active drain(s) and {ticket_count} resolved ticket(s)',
            'drains_cleared': count,
            'tickets_cleared': ticket_count
        })
    except Exception as e:
        return jsonify({
            'status': 'error',
            'error': str(e)
        }), 500


# Helper to get NVIDIA API key from environment
def _get_nv_api_key_from_env():
    """Retrieve NVIDIA API key from environment variables."""
    return os.environ.get('NV_API_KEY') or os.environ.get('nv_api_key') or os.environ.get('NVIDIA_API_KEY')


# --- Frontend Routes ---
@app.route('/')
def index():
    """Serves the new homepage."""
    # Serve the index.html from the project's root folder
    return send_from_directory(app.root_path, 'index.html')

@app.route('/loading')
def loading():
    """Serves the loading page that displays loading.gif before redirecting to dashboard.
    Allows demo mode without authentication, but requires auth for normal mode."""
    # Allow demo mode without auth
    if request.args.get('demo') == '1':
        return send_from_directory(app.root_path, 'loading.html')
    # Normal mode requires authentication
    if 'user' not in session:
        return redirect(url_for('login'))
    return send_from_directory(app.root_path, 'loading.html')

@app.route('/dashboard')
@requires_auth
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
@requires_auth
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
    # Bind explicitly to 'localhost' to match AUTH_CALLBACK_URL and
    # avoid callback mismatches when testing locally (localhost vs 127.0.0.1).
    app.run(host='localhost', debug=True, port=5000)