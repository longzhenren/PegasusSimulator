PX4 Integration
===============

The ``PX4-Autopilot`` support is provided by making use of the ``Control Backends API`` , and implementing a custom 
``PX4MavlinkBackend`` which contains a built-in tool to launch and kill PX4 in SITL mode automatically.

To instantiate a ``PX4MavlinkBackend`` via Python scripting, consider the following example:

.. code:: Python

    # Import the Mavlink backend module
    from pegasus.simulator.logic.backends.px4_mavlink_backend import PX4MavlinkBackend, PX4MavlinkBackendConfig

    # Create the multirotor configuration
    # In this example we are showing the default parameters that are used if you do not specify them
    mavlink_config = PX4MavlinkBackendConfig({"vehicle_id": 0,
        "connection_type": "tcpin",
        "connection_ip": "localhost",
        # The actual port that gets used = "connection_baseport" + "vehicle_id"
        "connection_baseport": 4560,
        "enable_lockstep": True,
        "num_rotors": 4,
        "input_offset": [0.0, 0.0, 0.0, 0.0],
        "input_scaling": [1000.0, 1000.0, 1000.0, 1000.0],
        "zero_position_armed": [100.0, 100.0, 100.0, 100.0],
        "update_rate": 250.0,

        # Settings for automatically launching PX4
        # If px4_autolaunch==False, then "px4_dir" and "px4_vehicle_model" are unused
        "px4_autolaunch": True,
        "px4_dir": "PegasusInterface().px4_path",
        "px4_vehicle_model": "iris",
        "sim_speed_factor": 1.0, # The speed factor for the simulation (e.g. 2.0 for 2x real-time)
        })
    config_multirotor.backends = [PX4MavlinkBackend(mavlink_config)]

.. note::

    In general, the Pegasus Simulator does not need to know where you have PX4 running to simulate the vehicle and send data 
    through ``MAVLink`` . However, if you intend to use the provided ``PX4 auto-launch`` feature, you must inform Pegasus Simulator
    where you have your local install of PX4.

By default, the simulator expects PX4 to be located at ``~/PX4-Autopilot`` directory. You can set the default 
path for the ``PX4-Autopilot`` by either:

1. Using the GUI of the Pegasus Simulator when operating in extension mode.


    .. image:: /_static/pegasus_GUI_px4_dir.png
        :width: 600px
        :align: center
        :alt: Setting the PX4 path

2. Use the methods provided by :class:`PegasusInterface`, i.e:

    .. code:: Python

        from pegasus.simulator.params import SIMULATION_ENVIRONMENTS
        from pegasus.simulator.logic.interface.pegasus_interface import PegasusInterface

        # Start the Pegasus Interface
        pg = PegasusInterface()

        # Set the default PX4 installation path used by the simulator
        # This will be saved for future runs
        pg.set_px4_path("path_to_px4_directory")

HTTP Control API (Examples)
---------------------------

For controlling vehicles from external processes, the examples include a ROS2 + MAVROS controller that exposes a small HTTP API while maintaining OFFBOARD control timing. See `examples/rospy_isaacsim.py`.

- Endpoints: `POST /reset`, `POST /command`, `POST /step`, `POST /step_http`, `GET /health`
- Concurrency: a task-level mutex ensures only one request executes at a time; `force:true` allows blocking acquisition.
- Image sources: can return images either from ROS2 topics or the simulation HTTP endpoint.
- Internal abstractions: `HttpApi` encapsulates route handlers; `ImageService` unifies image acquisition; `TaskGuard` standardizes lock handling.

This controller preserves the original takeoff/landing/reboot sequences and OFFBOARD setpoint timing while improving code readability and maintainability.

Examples (Python)
-----------------

Basic requests to `rospy_isaacsim.py` (default controller port `5009 + vehicle_id`):

.. code:: Python

    import requests

    base = "http://127.0.0.1:5009"  # For vehicle_id=0; adjust if MAVROS_NS is uavN

    # 1) Reset: optionally move vehicle in simulation, hard PX4 relaunch, and takeoff
    r = requests.post(base + "/reset", json={
        "vid": 0,
        "position": [0.0, 0.0, 1.0],
        "yaw_deg": 0.0,
        "hard": True,
        "force": True,
    }, timeout=60)
    print("reset:", r.status_code, r.json())

    # 2) Command: move_to
    r = requests.post(base + "/command", json={
        "cmd": "move_to",
        "x": 1.0, "y": -0.5, "z": 2.0,
        "force": True,
    }, timeout=30)
    print("move_to:", r.status_code, r.json())

    # 3) Command: move_to_many
    r = requests.post(base + "/command", json={
        "cmd": "move_to_many",
        "points": [[0.0, 0.0, 1.5], [0.5, 0.0, 1.5], [0.5, 0.5, 1.5]],
        "force": True,
    }, timeout=60)
    print("move_to_many:", r.status_code, r.json())

    # 4) Command: get_position
    r = requests.post(base + "/command", json={"cmd": "get_position"}, timeout=10)
    print("get_position:", r.status_code, r.json())

    # 5) Command: get_status
    r = requests.post(base + "/command", json={"cmd": "get_status"}, timeout=10)
    print("get_status:", r.status_code, r.json())

    # 6) Command: land
    r = requests.post(base + "/command", json={"cmd": "land", "force": True}, timeout=60)
    print("land:", r.status_code, r.json())

    # 7) Step: batch of waypoints (returns Base64 PNG frames)
    r = requests.post(base + "/step", json={
        "actions": [[[0.0, 0.0, 2.0], [0.5, 0.5, 2.0], [0.5, 0.0, 2.0]]],
        "per_step": False,
        "force": True,
    }, timeout=60)
    print("step:", r.status_code, r.json())

    # 8) Step_HTTP: same but get images from simulation HTTP (not ROS)
    r = requests.post(base + "/step_http", json={
        "actions": [[[0.0, 0.0, 2.0]]],
        "per_step": True,
        "force": True,
    }, timeout=60)
    print("step_http:", r.status_code, r.json())

    # 9) Health check
    r = requests.get(base + "/health", timeout=5)
    print("health:", r.status_code, r.json())

Examples (curl)
---------------

.. code:: bash

    # Reset (hard)
    curl -X POST "http://127.0.0.1:5009/reset" \
         -H "Content-Type: application/json" \
         -d '{"vid":0,"position":[0,0,1],"yaw_deg":0,"hard":true,"force":true}'

    # Move to
    curl -X POST "http://127.0.0.1:5009/command" \
         -H "Content-Type: application/json" \
         -d '{"cmd":"move_to","x":1,"y":-0.5,"z":2,"force":true}'

    # Move to many
    curl -X POST "http://127.0.0.1:5009/command" \
         -H "Content-Type: application/json" \
         -d '{"cmd":"move_to_many","points":[[0,0,1.5],[0.5,0,1.5],[0.5,0.5,1.5]],"force":true}'

    # Get position
    curl -X POST "http://127.0.0.1:5009/command" -H "Content-Type: application/json" -d '{"cmd":"get_position"}'

    # Get status
    curl -X POST "http://127.0.0.1:5009/command" -H "Content-Type: application/json" -d '{"cmd":"get_status"}'

    # Land
    curl -X POST "http://127.0.0.1:5009/command" -H "Content-Type: application/json" -d '{"cmd":"land","force":true}'

    # Step (ROS image)
    curl -X POST "http://127.0.0.1:5009/step" -H "Content-Type: application/json" \
         -d '{"actions":[[[0,0,2],[0.5,0.5,2]]],"per_step":false,"force":true}'

    # Step HTTP (simulation image)
    curl -X POST "http://127.0.0.1:5009/step_http" -H "Content-Type: application/json" \
         -d '{"actions":[[[0,0,2]]],"per_step":true,"force":true}'

Notes
-----

- Port: default is `5009 + vehicle_id`; can be overridden via `PEGASUS_HTTP_PORT`.
- Namespace: `MAVROS_NS` (e.g. `uav0`) determines vehicle id and ROS2 topic prefix.
- Concurrency: when a request is processing, another request returns `409 busy`; set `force:true` to block until the lock is acquired.
- Images: set `PEGASUS_IMAGE_FROM_ROS` and `PEGASUS_IMAGE_HTTP_URL` to select source; `per_step:true` returns a frame per waypoint.
