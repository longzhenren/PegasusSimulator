#!/bin/bash
# Firewall Configuration Script for Pegasus Simulator
# Configures firewall rules for network communication and ROS2

set -e

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m'

print_info() {
    echo -e "${GREEN}[INFO]${NC} $1"
}

print_warning() {
    echo -e "${YELLOW}[WARNING]${NC} $1"
}

print_error() {
    echo -e "${RED}[ERROR]${NC} $1"
}

echo -e "${GREEN}=== Firewall Configuration for Pegasus Simulator ===${NC}"

# Check if running as root
if [ "$EUID" -ne 0 ]; then
    print_error "This script must be run as root (use sudo)"
    exit 1
fi

# Configuration
PEGASUS_PORT=${1:-5555}
ROS2_PORT_START=7400
ROS2_PORT_END=7500

print_info "Configuring firewall for:"
print_info "  Pegasus Simulator port: $PEGASUS_PORT"
print_info "  ROS2 DDS ports: $ROS2_PORT_START-$ROS2_PORT_END"

# Detect firewall
if command -v ufw &> /dev/null; then
    print_info "Configuring UFW firewall..."

    # Enable UFW if not already enabled
    ufw --force enable

    # Allow Pegasus Simulator port
    ufw allow $PEGASUS_PORT/tcp comment 'Pegasus Simulator'
    print_info "Allowed TCP port $PEGASUS_PORT for Pegasus Simulator"

    # Allow ROS2 DDS ports
    ufw allow $ROS2_PORT_START:$ROS2_PORT_END/udp comment 'ROS2 DDS'
    ufw allow $ROS2_PORT_START:$ROS2_PORT_END/tcp comment 'ROS2 DDS'
    print_info "Allowed UDP/TCP ports $ROS2_PORT_START-$ROS2_PORT_END for ROS2"

    # Show status
    print_info "Current UFW status:"
    ufw status numbered

elif command -v firewall-cmd &> /dev/null; then
    print_info "Configuring firewalld..."

    # Allow Pegasus Simulator port
    firewall-cmd --permanent --add-port=$PEGASUS_PORT/tcp
    print_info "Allowed TCP port $PEGASUS_PORT for Pegasus Simulator"

    # Allow ROS2 DDS ports
    firewall-cmd --permanent --add-port=$ROS2_PORT_START-$ROS2_PORT_END/udp
    firewall-cmd --permanent --add-port=$ROS2_PORT_START-$ROS2_PORT_END/tcp
    print_info "Allowed UDP/TCP ports $ROS2_PORT_START-$ROS2_PORT_END for ROS2"

    # Reload firewall
    firewall-cmd --reload
    print_info "Firewall reloaded"

    # Show status
    print_info "Current firewall rules:"
    firewall-cmd --list-all

elif command -v iptables &> /dev/null; then
    print_info "Configuring iptables..."

    # Allow Pegasus Simulator port
    iptables -A INPUT -p tcp --dport $PEGASUS_PORT -j ACCEPT
    print_info "Allowed TCP port $PEGASUS_PORT for Pegasus Simulator"

    # Allow ROS2 DDS ports
    iptables -A INPUT -p udp --dport $ROS2_PORT_START:$ROS2_PORT_END -j ACCEPT
    iptables -A INPUT -p tcp --dport $ROS2_PORT_START:$ROS2_PORT_END -j ACCEPT
    print_info "Allowed UDP/TCP ports $ROS2_PORT_START-$ROS2_PORT_END for ROS2"

    # Save rules
    if command -v iptables-save &> /dev/null; then
        iptables-save > /etc/iptables/rules.v4
        print_info "Saved iptables rules"
    else
        print_warning "Could not save iptables rules permanently"
    fi

    # Show status
    print_info "Current iptables rules:"
    iptables -L -n

else
    print_warning "No supported firewall detected (ufw, firewalld, or iptables)"
    print_warning "Please configure your firewall manually:"
    echo "  - Allow TCP port $PEGASUS_PORT for Pegasus Simulator"
    echo "  - Allow UDP/TCP ports $ROS2_PORT_START-$ROS2_PORT_END for ROS2"
    exit 1
fi

echo ""
print_info "${GREEN}Firewall configuration complete!${NC}"
echo ""
echo "Configured ports:"
echo "  - TCP $PEGASUS_PORT: Pegasus Simulator network communication"
echo "  - UDP/TCP $ROS2_PORT_START-$ROS2_PORT_END: ROS2 DDS communication"
