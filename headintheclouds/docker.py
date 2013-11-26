import re
import simplejson as json
from fabric.api import sudo, hide

def inspect(process):
    with hide('output'):
        result = sudo('docker inspect %s' % process)
    if result.failed:
        return None
    return json.loads(result)

def bind(process, port):
    info = inspect(process)
    ip = info[0]['NetworkSettings']['IPAddress']
    import ipdb; ipdb.set_trace()
    unbind(port)
    sudo('iptables -t nat -A DOCKER -p tcp --dport %s -j DNAT --to-destination %s:%s' % (port, ip, port))

def unbind(port):
    rules = sudo('iptables -t nat -S')
    for rule in rules.split('\r\n'):
        if re.search('^-A DOCKER -p tcp -m tcp --dport %s' % port, rule):
            undo_rule = re.sub('-A DOCKER', '-D DOCKER', rule)
            import ipdb; ipdb.set_trace()
            sudo('iptables -t nat %s' % undo_rule)
