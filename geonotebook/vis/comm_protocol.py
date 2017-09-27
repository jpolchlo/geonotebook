import pickle
import zmq
import tempfile
import os

def webapp_comm_send(msg):
    ctx = zmq.Context()
    s = ctx.socket(zmq.REQ)

    with open("{}/{}".format(tempfile.gettempdir(), os.environ['USER']), 'r') as f:
        comm_port = int(f.read())

    s.connect("tcp://127.0.0.1:{}".format(comm_port))
    s.send(pickle.dumps(msg))
    resp = pickle.loads(s.recv())
    s.close()
    return resp

def make_callback(transport, protocol):
    def _recv(msg):
        result = protocol(msg)
        protocol.nbapp.log.info("Sending response: {}".format(result))
        transport.send(pickle.dumps(result))

    return _recv
        
class WebAppProtocol(object):
    def __init__(self, nbapp):
        self.nbapp = nbapp

    def __call__(self, msg):
        data = pickle.loads(msg[0])
        self.nbapp.log.info("ZMQ socket received data={}".format(data))
        action =  data.get('action', 'err_missing_action')
        try:
            return getattr(self, action)(**data)        
        except Exception as e:
            return {
                'success': False,
                'err_msg': 'Unidentified protocol error: {}'.format(str(e))
            }
                               
    def register_kernel(self, **data):
        kernel_id = data['kernel_id']
        kwargs = {} if 'json' not in data else data['json']
        try:
            self.nbapp.web_app.ktile_config_manager.add_config(kernel_id, **kwargs)
            return { 'success': True }
        except Exception as exc:
            return { 'success': False,
                     'err_msg': exc
            }

    def delete_kernel(self, **data):
        kernel_id = data['kernel_id']
        try:
            del self.nbapp.web_app.ktile_config_manager[kernel_id]
            return { 'success': True }
        except KeyError:
            return { 'success': False,
                     'err_msg': u'Kernel %s not found' % kernel_id
            }

    def add_layer(self, **data):
        try:
            kernel_id = data['kernel_id']
            layer_name = data['layer_name']
            self.nbapp.web_app.ktile_config_manager.add_layer(kernel_id, layer_name, data['json'])
            return { 'success': True }
        except Exception:
            import sys
            import traceback
            t, v, tb = sys.exc_info()
            return { 'success': False,
                     'err_msg': traceback.format_exception(t, v, tb)
            }

    def request_port(self, **data):
        return { 'port': self.nbapp.port }

    def request_base_url(self, **data):
        return { 'base_url': self.nbapp.web_app.settings['base_url'] }
            
    def err_missing_action(self, **data):
        return {
            'success': False,
            'err_msg': 'Unrecognized request, no action'
        }
