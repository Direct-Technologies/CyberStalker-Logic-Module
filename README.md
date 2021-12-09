## CyberStalker-Logic-Module

Created to improve user’s predictive cybersecurity analytics, CyberStalker Logic Module allows the implementation of both standard and custom rules to ensure the best security threat analysis. The module provides you with the opportunity to administrate thresholds, customize them under specific activities and needs so the user cannot miss any minor suspicious event within the infrastructure. Moreover, the slightest potential problems can be identified even within a hybrid IT environment.
What is more, the module combines several sandboxing approaches that help the user to detect new forms of malware, prevent zero-day attacks, detect targeted malware, and deliver the highest detection rates in the cybersecurity industry.
Its key feature - the advanced scan engine - employs advanced decomposition techniques that extract and reveal all compressed and obfuscated data in a malicious file in real time, thus providing next-level security for the infrastructure in use.

----
### How to

To build the docker image:
```
make docker
```
To push the docker image to the pixel registry:
```
make deploy
```

----
### Dispatcher

The business logic dispatcher handles all the business logic tasks of the Pixel Core platform. It is built as a collection of subscriptions, recurring jobs and asynchronous tasks called workers.

On startup the dispatcher will start a set of subscriptions listening for events. When an event is received it triggers an asynchronous task designed to process the event, update affected objects accordingly and execute external tasks if necessary. 

An example would be sending email notifications. A subscription listens for new notifications. When a new notification event is received an asynchronous task is created which process the notification by looking for the users eligible to receive this notification as an email, sending the email notification and report on the delivery status of this email notification.

The dispatcher will also start recurring jobs that run at a fixed time interval.

An example would be the coordinates resolution job. Every few seconds a task is run that looks for all objects in the system eligible for coordinates resolution. It then applies the coordinate resolution algorithm to each of them and update the objects coordinates when necessary. It then wait for the next cycle.

The dispatcher is also responsible to provision on startup all the schemas and media objects for the Pixel Core applications.

A short description of all the workers and RPCs is given in the next section. The technical description of the workers is to be found in the pydocs.

----
### General workers

----
### Admin app workers

----
### Board app workers

#### Counter statistics

```yaml
meta: 
  file: ./dispatcher/dispatcher_workers/counter_statistics.py
  task_name: counter_statistics_sub_${counter statistics object id}
config:
  source_schema: Business Logic Dispatcher
  type: worker
  default_enabled: true
inputs:
  input_1:
    type: subscription
    filter:
      tags: 
        - application
        - dispatcher
        - statistics
        - counter
      propertyChanged: 
        - Settings/SchemaId
        - Settings/Linked only
        - Settings/Filter
  input_2:
    type: query
    filter:
      schemaId: ${from counter statistics object filter}
outputs:
  output_1:
    type: mutation
    object: ${counter statistics object}
    properties:
      - Value/Value
      - Value/Percentage 
```

This worker manages the counter statistics objects (tags `["application", "dispatcher", "statistics", "counter"]`). These counter statistics objects count the number of objects with a given schema id that satisfy a set of filtering conditions.

On startup the worker looks for all the counter statistics objects and creates a task for each of them. Each task is responsible to update the count and percentage of objects that are described by the settings of the counter statistics object.

The settings of the counter statistics objects allows for filtering the objects to be counted by schema id, conditions on properties and whether or not to restrict only to objects linked to the counter statistics object:
```json
"properties": [
        {
            "type": "string",
            "property": "SchemaId",
            "group_name": "Settings",
            "description": "SchemaId"
        },
        {
            "type": "json object",
            "property": "Filter",
            "group_name": "Settings",
            "description": "Filter",
            "default_value": {
                "filtering": false,
                "conditions": [
                    {
                        "property": "group/property",
                        "operator": "==",
                        "value": 0
                    }
                ]
            }
        },
        {
            "type": "bool",
            "property": "Linked only",
            "group_name": "Settings",
            "description": "Linked only"
        }
    ]
```

After startup the worker listens for creation, configuration updates and deletion of the counter statistics objects:
- On creation a new task is started to update the count and percentage changes.
- On configuration update the corresponding old task is canceled and a new one using the new settings is started.
- On deletion the corresponding task is canceled.

#### Objects statistics

```yaml
meta: 
  file: ./dispatcher/dispatcher_workers/objects_statistics.py
  task_name: objects_statistics_sub_${objects statistics object id}
config:
  source_schema: Business Logic Dispatcher
  type: worker
  default_enabled: true
inputs:
  input_1:
    type: subscription
    filter:
      tags: 
        - application
        - dispatcher
        - statistics
        - objects
      propertyChanged: 
        - Settings/Function
        - Settings/Filter
        - Settings/Linked only
        - Settings/Property
        - Settings/SchemaId
        - Settings/Custom function
  input_2:
    type: subscription
    filter:
      id: ${array of object ids whose schema id matches Settings/SchemaId}
      propertyChanged:
        - ${property matching Settings/Property}
outputs:
  output_1:
    type: mutation
    object: ${objects statistics object}
    properties:
      - Value/Value
```

This worker manages the objects statistics objects (tags `["application", "dispatcher", "statistics", "objects"]`). These objects statistics objects gather the values of a given property shared by a set of objects. A statistics function is then applied to this set of values and the result is stored in the objects statistics object.

On startup the worker looks for all the objects statistics objects and creates a task for each of them. Each task is responsible to update the statistics value of objects that are described by the settings of the objects statistics object.

The settings of the objects statistics objects allows for filtering the objects to be targetted by schema id, conditions on properties and whether or not to restrict only to objects linked to the objects statistics object:

```json
"properties": [
        {
            "type": "string",
            "property": "SchemaId",
            "group_name": "Settings",
            "description": "SchemaId"
        },
        {
            "type": "string",
            "property": "Property",
            "group_name": "Settings",
            "description": "Property"
        },
        {
            "type": "json object",
            "property": "Filter",
            "group_name": "Settings",
            "description": "Filter",
            "default_value": {
                "filtering": false,
                "conditions": [
                    {
                        "property": "group/property",
                        "operator": "==",
                        "value": 0
                    }
                ]
            }
        },
        {
            "type": "bool",
            "property": "Linked only",
            "group_name": "Settings",
            "description": "Linked only"
        }
    ]
```

The statistics function to be applied to the property values can be either picked from a set of predefined functions or be a custom function coded in javascript (ECMA 5.1 standard):

```json
"properties": [
        {
            "type": "string",
            "property": "Function",
            "value_set": {
                "list": [
                    {
                        "key": "Average",
                        "title": "Average"
                    },
                    {
                        "key": "Min",
                        "title": "Min"
                    },
                    {
                        "key": "Max",
                        "title": "Max"
                    },
                    {
                        "key": "Custom",
                        "title": "Custom"
                    }
                ],
                "component": "select"
            },
            "group_name": "Settings",
            "description": "Function",
            "default_value": "Max",
        },
        {
            "type": "json object",
            "property": "Custom function",
            "group_name": "Settings",
            "description": "Custom javascript function",
            "default_value": {"code": "function f(values) {return values.reduce(function(acc, val) { return acc + val; }, 0)}"},
        }
    ]
```

After startup the worker listens for creation, configuration updates and deletion of the objects statistics objects:
- On creation a new task to update the statistics.
- On configuration update the corresponding old task is canceled and a new one using the new settings is started.
- On deletion the corresponding task is canceled.

#### Timeseries statistics

```yaml
meta: 
  file: ./dispatcher/dispatcher_workers/timeseries_statistics.py
  task_name: timeseries_statistics_sub_${timeseries statistics object id}
config:
  source_schema: Business Logic Dispatcher
  type: worker
  default_enabled: true
inputs:
  input_1:
    type: subscription
    filter:
      tags: 
        - application
        - dispatcher
        - statistics
        - timeseries
      propertyChanged: 
        - Settings/SchemaId
        - Settings/Linked only
        - Settings/Filter
        - Settings/Period seconds
  input_2:
    type: query
    filter:
      schemaId: ${from timeseries statistics object filter}
outputs:
  output_1:
    type: mutation
    object: ${timeseries statistics object}
    properties:
      - Value/Value
      - Value/Timeseries 
```

----
### Monitor app workers

----
### OLD ==============>

#### Calculate object property statistics

```yaml
meta: 
  file: ./dispatcher/dispatcher_workers/calculate_object_property_statistics.py
config:
  type: worker
  enabled: true
```

#### Notification delivery 

```yaml
meta: 
  file: ./dispatcher/dispatcher_workers/notification_delivery.py
config:
  type: worker
  enabled: true
```

This worker is responsible to deliver the notifications generated by the platform to users through a set of delivery paths:
* Platform applications
* Email
* SMS
* WhatsApp

On startup, the worker creates a subscription (on behalf of the dispatcher) listening on all `Notifications`.

When a notification event is received through the subscription, an asynchronous tasks is created for each delivery path using the event's related node as imput.

#### Resolve coordinates

```yaml
meta: 
  file: ./dispatcher/dispatcher_workers/resolve_coordinates.py
config:
  type: job
  timer: 10
  enabled: true
```

At regular time intervals this job looks for all the monitor objects that are in need of coordinates resolution. 

Then for each monitor object the coordinates resolution algorithm tries to resolve the position of the object using attached GPS, scanners and beacons data. If the position of the monitor object has changed it then gets updated.

#### Resolve monitor object global alarms

```yaml
meta: 
  file: ./dispatcher/dispatcher_workers/resolve_monitor_object_global_alarms.py
config:
  type: worker
  enabled: true
```

Listens for alarms changes on monitoring items. Every time an alarm state is changed on the monitoring items of a monitor object this worker decides of the monitor object should be alarmed or not:
* If at least one item alarm is triggered, the object alarm is triggered.
* If no item alarm is triggered, the object alarm is turned off.

#### Resolve monitor object item alarms

```yaml
meta: 
  file: ./dispatcher/dispatcher_workers/resolve_monitor_object_item_alarms.py
config:
  type: worker
  enabled: true
```

Listens for changes in the value properties of monitoring items. Every time the value is updated the alarms conditions are evaluated against it. If at least one condition evaluates to true (after the alarm delay) the item alarm gets triggered and a notification is generated. If all conditions evaluate to false then the item alarm is immediately turned off.

#### Resolve monitor object item geo alarms

```yaml
meta: 
  file: ./dispatcher/dispatcher_workers/resolve_monitor_object_item_geo_alarms.py
config:
  type: worker
  enabled: true
```

Listens for changes in the position of a monitor object. Every time the position is updated the alarms conditions are evaluated against it. If at least one condition evaluates to true (after the alarm delay) the item alarm gets triggered and a notification is generated. If all conditions evaluate to false then the item alarm is immediately turned off.

#### Resolve monitor object state

```yaml
meta: 
  file: ./dispatcher/dispatcher_workers/resolve_monitor_object_state_generic.py
config:
  type: worker
  enabled: true
```

Listens for changes in the property linked to a monitor object state. Every time this propery is updated the states are evaluated in order. The first state to evaluate to true becomes the new state of the monitor object. If no state evaluates to true then the monitor object is put in the default state.

#### Resolve monitor object statuses

```yaml
meta: 
  file: ./dispatcher/dispatcher_workers/resolve_monitor_object_statuses.py
config:
  type: job
  timer: 10
  enabled: true
```

At regular time intervals this job looks for all the monitor objects to resolve object level statuses (battery level, response status, gps status). 

On each cycle the job gets the latest property values from the monitor object and its connected objects. It then updated the monitor object statuses if necessary.

#### Statistics

```yaml
meta: 
  file: ./dispatcher/dispatcher_workers/statistics.py
config:
  type: worker
  enabled: true
```

This worker listens for the creations and updates of statistics objects. Every time a statistics object is updated the corresponding statistics subscription is restarted. 

Each statistics subscription listens for a specific property update on which statistics are calculated for every update. The result of those calculations are stored in the statistics object.

----
### RPCs

#### Add monitor objects

```yaml
meta: 
  file: ./dispatcher/dispatcher_workers/add_monitor_objects.py
```

Targets a set of devices to be provisioned as monitor objects in bulk. 

#### Restart task

```yaml
meta: 
  file: ./dispatcher/dispatcher_workers/restart_task.py
```

Targets an asynchronous task running in the dispatcher's event loop by its name. If the named task is currently running, it gets cancelled and restarted. If no task with that name can be found a new task is created.

Note that the restart task RPC should only be applied to tasks that do not take any input on starting.

#### Test module

```yaml
meta: 
  file: ./dispatcher/dispatcher_workers/test_module.py
```

Runs the health checks on the dispatcher and reports pass or fail for each of them.




