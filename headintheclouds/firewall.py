from fabric.api import * # pylint: disable=W0614,W0401

CHAIN = 'HEAD_IN_THE_CLOUDS'

def set_rules(open_list):
    c = [] # list of commands we will join with &&

    if has_chain():
        c.append(flush_chain)
    else:
        c.append(make_chain)
        c.append(jump_to_chain)

    c.append(drop_null_packets)
    c.append(drop_syn_flood)
    c.append(drop_xmas_packets)
    c.append(accept_loopback)

    # allow dns ports
    c.append(accept(53, None, 'tcp', None))
    c.append(accept(53, None, 'udp', None))

    for source_port, destination_port, protocol, addresses in open_list:
        c.append(accept(source_port, destination_port, protocol, addresses))

    c.append(accept_established)

    c.append(drop_all)

    cmd = ' && '.join(c)
    sudo(cmd)

def has_chain():
    with settings(hide('everything'), warn_only=True):
        return not run('iptables --list %s' % CHAIN).failed

def accept(source_port, destination_port, protocol, addresses):
    protocol = protocol or 'tcp'

    if addresses:
        source = '--source %s' % ','.join(addresses)
    else:
        source = ''
    if source_port:
        sport = '-p %s --sport %s' % (protocol, source_port)
    else:
        sport = ''
    if destination_port:
        dport = '-p %s --dport %s' % (protocol, destination_port)
    else:
        dport = ''

    return 'iptables -A %s %s %s %s -j RETURN' % (CHAIN, source, sport, dport)

flush_chain        = 'iptables --flush %s' % CHAIN
make_chain         = 'iptables --new-chain %s' % CHAIN
jump_to_chain      = 'iptables -A INPUT -j %s' % CHAIN
drop_null_packets  = 'iptables -A %s -p tcp --tcp-flags ALL NONE -j DROP' % CHAIN
drop_syn_flood     = 'iptables -A %s -p tcp ! --syn -m state --state NEW -j DROP' % CHAIN
drop_xmas_packets  = 'iptables -A %s -p tcp --tcp-flags ALL ALL -j DROP' % CHAIN
accept_loopback    = 'iptables -A %s -i lo -j RETURN' % CHAIN
accept_established = 'iptables -A %s -m state --state RELATED,ESTABLISHED -j RETURN' % CHAIN
drop_all           = 'iptables -A %s -j DROP' % CHAIN
delete_jump        = 'iptables -D INPUT -j %s' % CHAIN
delete_chain       = 'iptables --delete-chain %s' % CHAIN

class FirewallException(Exception):
    pass
