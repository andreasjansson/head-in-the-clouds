from fabric.api import * # pylint: disable=W0614,W0401

CHAIN = 'HEAD_IN_THE_CLOUDS'

def set_rules(open_list, from_chain='INPUT'):
    rules = make_rules(open_list, from_chain)
    rules = ['iptables ' + r for r in rules]
    cmd = ' && '.join(rules)
    sudo(cmd)

def make_rules(open_list, from_chain='INPUT'):
    c = [] # list of commands we will join with &&

    if has_chain():
        c.append(flush_chain)
    else:
        c.append(make_chain)
        c.append(jump_to_chain(from_chain))

    c.append(drop_null_packets)
    c.append(drop_syn_flood)
    c.append(drop_xmas_packets)
    c.append(accept_loopback)

    # allow dns ports
    c += accept(53, None, 'tcp', None)
    c += accept(53, None, 'udp', None)

    for source_port, destination_port, protocol, addresses in open_list:
        c += accept(source_port, destination_port, protocol, addresses)

    c.append(accept_established)

    c.append(drop_all)
    return c

def get_rules():
    with settings(hide('everything'), warn_only=True):
        rules = sudo('iptables -S %s' % CHAIN)

    rules = rules.splitlines()
    rules = [r for r in rules if r != make_chain]

    return rules

def rules_are_active(open_list, from_chain='INPUT'):
    new_rules = make_rules(open_list, from_chain)
    new_rules = [r for r in new_rules if r != flush_chain]
    existing_rules = get_rules()

    return new_rules == existing_rules

def has_chain():
    with settings(hide('everything'), warn_only=True):
        return not sudo('iptables -L %s' % CHAIN).failed

def accept(source_port, destination_port, protocol, raw_addresses):
    '''
    accepts comma separated addresses or list of addresses
    '''

    protocol = protocol or 'tcp'

    if not isinstance(raw_addresses, list):
        raw_addresses = [raw_addresses]

    addresses = []
    for a in raw_addresses:
        if a is None:
            addresses.append(None)
        else:
            addresses += a.split(',')

    rules = []
    for address in addresses:
        parts = ['-A', CHAIN]

        if address:
            address, _, mask = address.partition('/')
            mask = mask or '32'
            parts.append('-s %s/%s' % (address, mask))

        if source_port:
            parts.append('-p %s -m %s --sport %s' % (protocol, protocol, source_port))

        if destination_port:
            parts.append('-p %s -m %s --dport %s' % (protocol, protocol, destination_port))

        parts += ['-j', 'RETURN']

        rules.append(' '.join(parts))

    return rules

def jump_to_chain(from_chain='INPUT'):
    return '-A %s -j %s' % (from_chain, CHAIN)

def delete_jump(from_chain='INPUT'):
    return '-D %s -j %s' % (from_chain, CHAIN)

flush_chain        = '-F %s' % CHAIN
make_chain         = '-N %s' % CHAIN
drop_null_packets  = '-A %s -p tcp -m tcp --tcp-flags FIN,SYN,RST,PSH,ACK,URG NONE -j DROP' % CHAIN
drop_syn_flood     = '-A %s -p tcp -m tcp ! --tcp-flags FIN,SYN,RST,ACK SYN -m state --state NEW -j DROP' % CHAIN
drop_xmas_packets  = '-A %s -p tcp -m tcp --tcp-flags FIN,SYN,RST,PSH,ACK,URG FIN,SYN,RST,PSH,ACK,URG -j DROP' % CHAIN
accept_loopback    = '-A %s -i lo -j RETURN' % CHAIN
accept_established = '-A %s -m state --state RELATED,ESTABLISHED -j RETURN' % CHAIN
drop_all           = '-A %s -j DROP' % CHAIN
delete_chain       = '-X %s' % CHAIN

class FirewallException(Exception):
    pass
