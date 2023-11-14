from event import Event
import json
import datetime
import traceback


def json_serialize_others(obj):
    """JSON serializer for objects not serializable by default json code"""

    if isinstance(obj, (datetime.datetime, datetime.date)):
        return obj.isoformat()
    else:
        print(
            f"Error in json.dumps() of decorator: type {type(obj)} not serializable, of object {obj}")
        return str(obj)


def prepare_response(response):
    if isinstance(response, str):
        try:
            response = json.loads(response)
        except json.JSONDecodeError:
            ...
    else:
        ...

    return json.dumps(response)


def run_and_standardize_response(f, event, context, **kwargs):
    # 2. call endpoint
    try:
        response = f(event, context)

        # 3. standardize response
        if not (type(response) is dict and 'statusCode' in response and 'body' in response):
            response = {
                'statusCode': 200,
                'headers': {
                    "Content-Type": kwargs["output_mime"],
                    'Access-Control-Allow-Origin': '*'
                },
                'body': prepare_response(response)
            }
        else:
            response['body'] = prepare_response(response['body'])
            response['headers'] = {
                "Content-Type": kwargs["output_mime"],
                'Access-Control-Allow-Origin': '*'
            }

        errors = {}
    except Exception as e:
        print(f"""An error occurred: {e}""")
        traceback.print_exc()
        response = {
            'statusCode': 500,
            'headers': {
                'Access-Control-Allow-Origin': '*'
            },
            'body': str(e)
        }
        msg = traceback.format_exc()
        errors = {
            'exceptionType': str(type(e)),
            'exceptionMessage': str(e),
            'traceback': msg
        }

    return response, errors


def lynx(output_mime='application/json'):
    """
    """
    def decorator(f):
        def wrapper(event, context):
            # 1. transform the event
            event = Event(event)

            # Call endpoint
            response, _ = run_and_standardize_response(
                f, event, context, output_mime=output_mime)

            event.rds.close()

            return response
        return wrapper
    return decorator
