from flask import Flask, jsonify, request, render_template
from flask_cors import CORS
import random
import time

# --- Setup ---
app = Flask(__name__)
# This allows your frontend (on a different port) to call your backend
CORS(app) 

# --- Mock Factory Data (The EOG Data Streams) ---
# In a real app, this data would be constantly updating from a database or stream
mock_cauldrons = {
    "c1": {"name": "Gryffin's Gold", "level": 85, "capacity": 100},
    "c2": {"name": "Serpent's-Strength", "level": 92, "capacity": 100},
    "c3": {"name": "Raven's-Wit", "level": 50, "capacity": 100, "anomaly": False},
    "c4": {"name": "Badger's-Calm", "level": 70, "capacity": 100},
}

# We'll simulate a suspicious drop in Cauldron 3
mock_cauldron_level_history = {
    "c3": [
        {"time": 1731090000, "level": 75}, # 2:20 PM
        {"time": 1731090600, "level": 50}, # 2:30 PM (25L drop!)
        {"time": 1731091200, "level": 50}, # 2:40 PM
    ]
}

mock_transport_tickets = [
    {"ticket_id": "T-001", "cauldron_id": "c1", "amount": 80, "time": 1731088200},
    # Notice no ticket for the 25L drop in c3
]

# --- EOG Challenge: Tool Definitions (API Endpoints) ---

@app.route('/api/cauldron/levels')
def get_cauldron_levels():
    """Tool: Gets the current level of all cauldrons."""
    # Simulate real-time updates for the demo
    for cid in mock_cauldrons:
        if cid != "c3": # Don't mess with our anomaly
            mock_cauldrons[cid]["level"] += random.randint(-1, 2)
            if mock_cauldrons[cid]["level"] > 98:
                 mock_cauldrons[cid]["level"] = 98
            if mock_cauldrons[cid]["level"] < 20:
                mock_cauldrons[cid]["level"] = 20
                
    return jsonify(mock_cauldrons)

@app.route('/api/tickets/check_discrepancies')
def check_discrepancies():
    """
    Tool: The core EOG logic.
    Checks cauldron history for large drops NOT matching a transport ticket.
    """
    alerts = []
    # This is a simplified check for the hackathon:
    # We'll just check our hard-coded c3 history
    history = mock_cauldron_level_history["c3"]
    drop = history[0]["level"] - history[1]["level"] # 75 - 50 = 25
    
    if drop > 0:
        found_ticket = False
        for ticket in mock_transport_tickets:
            if (ticket["cauldron_id"] == "c3" and 
                ticket["time"] > history[0]["time"] and 
                ticket["time"] < history[2]["time"]):
                found_ticket = True
                break
        
        if not found_ticket:
            mock_cauldrons["c3"]["anomaly"] = True # Flag it in our "database"
            alerts.append({
                "cauldron_id": "c3",
                "message": f"Suspicious 25L drop detected in {mock_cauldrons['c3']['name']} with no matching transport ticket."
            })

    return jsonify(alerts)

@app.route('/api/logistics/forecast')
def forecast_fill_times():
    """
    Tool (EOG Bonus): Forecasts fill times.
    This is a simple linear forecast.
    """
    forecasts = []
    # Simplified logic: 1% fill per "tick"
    for cid, data in mock_cauldrons.items():
        if data["level"] < 98: # Don't forecast full ones
            time_to_full = (data["capacity"] - data["level"]) * 2 # (2 mins per %)
            forecasts.append({
                "cauldron_id": cid,
                "name": data["name"],
                "time_to_full_min": time_to_full
            })
    
    return jsonify(forecasts)

@app.route('/api/logistics/dispatch_courier', methods=['POST'])
def dispatch_courier():
    """
    Tool (NVIDIA Action): Dispatches a courier witch.
    This is an "action" tool.
    """
    data = request.json
    cauldron_id = data.get('cauldron_id')
    
    if not cauldron_id or cauldron_id not in mock_cauldrons:
        return jsonify({"status": "error", "message": "Invalid cauldron ID."}), 400
    
    # Simulate dispatching
    mock_cauldrons[cauldron_id]["level"] = 0 # Emptied
    print(f"DISPATCH: Courier dispatched to {mock_cauldrons[cauldron_id]['name']}")
    
    return jsonify({
        "status": "success",
        "message": f"Courier witch dispatched to {mock_cauldrons[cauldron_id]['name']}. It is now being emptied."
    })


# --- NVIDIA Challenge: The Agent "Brain" (Controller) ---
# This is where you would call the Nemotron API

@app.route('/api/agent/chat', methods=['POST'])
def handle_agent_chat():
    user_message = request.json.get('message')

    # 1. Define the tools your agent can use (these are your API endpoints)
    # You will show this definition to the Nemotron model
    tools_schema = [
        {
            "name": "check_discrepancies",
            "description": "Checks all cauldrons for suspicious potion drops that don't have a matching transport ticket.",
            "parameters": {} # No parameters needed
        },
        {
            "name": "forecast_fill_times",
            "description": "Forecasts the time in minutes until each non-full cauldron overflows.",
            "parameters": {} # No parameters needed
        },
        {
            "name": "dispatch_courier",
            "description": "Dispatches a courier witch to a specific cauldron to empty it and prevent overflow.",
            "parameters": {
                "type": "object",
                "properties": {
                    "cauldron_id": {"type": "string", "description": "The ID of the cauldron (e.g., 'c1', 'c2')."}
                },
                "required": ["cauldron_id"]
            }
        }
    ]

    # 2. Call Nemotron (PSEUDO-CODE)
    # This is the "Reasoning" step for NVIDIA
    # You'd send the user_message, chat history, and tools_schema
    # nemotron_response = nemotron.chat(
    #     message=user_message,
    #     tools=tools_schema
    # )

    # 3. Simulate the Nemotron Response & Tool-Use
    # For the hackathon, we'll write simple "if" statements to simulate the agent's "reasoning"
    # This proves the NVIDIA "multi-step workflow" concept.
    
    agent_plan = [] # To show the multi-step plan
    agent_final_response = ""

    # SIMULATION 1: User asks for anomalies
    if "suspicious" in user_message.lower() or "anomaly" in user_message.lower() or "discrepancies" in user_message.lower():
        agent_plan.append("Plan: User asked for anomalies. I will call `check_discrepancies()`.")
        
        # Call the tool (which is just our local function)
        alerts = check_discrepancies().get_json() 
        
        if alerts:
            agent_plan.append("Tool Result: Found anomalies.")
            agent_final_response = f"I found an alert: {alerts[0]['message']}"
        else:
            agent_plan.append("Tool Result: No anomalies found.")
            agent_final_response = "All potion flows are accounted for. No discrepancies found."

    # SIMULATION 2: User asks for forecasts (EOG Bonus)
    elif "forecast" in user_message.lower() or "full" in user_message.lower():
        agent_plan.append("Plan: User asked for forecasts. I will call `forecast_fill_times()`.")
        
        # Call the tool
        forecasts = forecast_fill_times().get_json()
        agent_plan.append(f"Tool Result: {forecasts}")
        
        agent_final_response = "Here is the forecast:\n"
        for f in forecasts:
            agent_final_response += f"  - {f['name']} ({f['cauldron_id']}) will be full in {f['time_to_full_min']} minutes.\n"

    # SIMULATION 3: User wants to TAKE ACTION (NVIDIA Agentic)
    elif "dispatch" in user_message.lower() or "empty" in user_message.lower():
        # Simple parsing to find the cauldron ID
        cauldron_id_to_dispatch = None
        for cid in mock_cauldrons.keys():
            if cid in user_message.lower():
                cauldron_id_to_dispatch = cid
                break
        
        if cauldron_id_to_dispatch:
            agent_plan.append(f"Plan: User wants to dispatch to {cauldron_id_to_dispatch}. I will call `dispatch_courier()`.")
            
            # Call the "action" tool
            # In a real app, we'd make a proper POST request to our own endpoint
            # For simplicity, we just modify the mock data directly
            mock_cauldrons[cauldron_id_to_dispatch]["level"] = 0
            mock_cauldrons[cauldron_id_to_dispatch]["anomaly"] = False # Clear anomaly on dispatch
            
            agent_plan.append("Tool Result: Dispatch successful.")
            agent_final_response = f"Action taken. A courier has been dispatched to {mock_cauldrons[cauldron_id_to_dispatch]['name']}."
        else:
            agent_final_response = "Which cauldron (e.g., c1, c2) should I dispatch to?"

    else:
        agent_final_response = "I can help with: checking for **suspicious** activity, **forecasting** fill times, or **dispatching** couriers (e.g., 'dispatch to c2')."

    # Return the agent's thought process (for the judges) and the final answer
    return jsonify({
        "agent_response": agent_final_response,
        "agent_plan": agent_plan # This is great for your NVIDIA demo!
    })


# --- Frontend Routes (UPDATED) ---
@app.route('/')
def index():
    """Serves the new homepage."""
    return render_template('index.html')

@app.route('/dashboard')
def dashboard():
    """Serves the main Poyolab dashboard app."""
    return render_template('dashboard.html')

if __name__ == '__main__':
    # We'll serve the HTML from Flask for simplicity
    app.run(debug=True, port=5000)