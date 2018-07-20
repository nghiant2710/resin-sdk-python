from functools import wraps
import json
import threading

from .base_request import BaseRequest
from . import exceptions
from .models.config import Config
from .models.device import Device
from .settings import Settings


class Subscription(threading.Thread):
    """
    This is low level class and is not meant to be used by end users directly.
    """

    def __init__(self, response, callback):
        self.response = response
        self.callback = callback
        super(Subscription, self).__init__()

    def run(self):
        if (self.response.status_code == 200):
            try:
                for line in self.response.iter_lines():
                    if line:
                        self.callback(json.loads(line))
            except AttributeError:
                # expected AttributeError when response.closed() called
                pass
        else:
            raise exceptions.RequestError(response.content)

    def stop(self):
        # Releases the connection back to the pool and response.raw cannot be accessed again so AttributeError will be raised if trying to read from raw.
        self.response.close()

class Logs(object):
    """
    This class implements functions that allow processing logs from device.

    """

    subscriptions = {}

    def __init__(self):
        self.base_request = BaseRequest()
        self.config = Config()
        self.device = Device()
        self.settings = Settings()

    def subscribe(self, uuid, callback):
        """
        This function allows subscribing to device logs.

        Args:
            uuid (str): device uuid.
            callback (function): this callback is called on receiving a message from the channel.

        """

        raw_query = 'stream=1'

        resp = self.base_request.request(
            '/device/v2/{uuid}/logs'.format(uuid=uuid), 'GET', raw_query=raw_query, stream=True,
            endpoint=self.settings.get('api_endpoint')
        )

        if uuid not in self.subscriptions:
            self.subscriptions[uuid] = Subscription(resp, callback)
            self.subscriptions[uuid].start()

    def history(self, uuid, count=None):
        """
        Get device logs history.
        Args:
            uuid (str): device uuid.
            count (int): this callback is called on receiving a message from the channel.

        """

        raw_query = ''

        if count:
            raw_query = 'count={}'.format(count)

        return self.base_request.request(
            '/device/v2/{uuid}/logs'.format(uuid=uuid), 'GET', raw_query=raw_query,
            endpoint=self.settings.get('api_endpoint')
        )

    def unsubscribe(self, uuid=None):
        if uuid and uuid in self.subscriptions:
            self.subscriptions.pop(uuid, None).stop()
        else:
            # unsubscribe all if no uuid specified
            for item in self.subscriptions:
                self.subscriptions[item].stop()
            self.subscriptions = {}

