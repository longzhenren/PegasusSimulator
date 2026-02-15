#!/bin/bash
# ROS2 Network Configuration Script
# This script configures ROS2 for remote access across different hosts

set -e

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m' # No Color

echo -e "${GREEN}=== ROS2 Network Configuration ===${NC}"

# Function to print colored messages
print_info() {
    echo -e "${GREEN}[INFO]${NC} $1"
}

print_warning() {
    echo -e "${YELLOW}[WARNING]${NC} $1"
}

print_error() {
    echo -e "${RED}[ERROR]${NC} $1"
}

# Check if ROS2 is installed
if ! command -v ros2 &> /dev/null; then
    print_error "ROS2 is not installed or not in PATH"
    exit 1
fi

print_info "ROS2 found: $(ros2 --version)"

# Get ROS2 distro
ROS_DISTRO=${ROS_DISTRO:-humble}
print_info "ROS2 distro: $ROS_DISTRO"

# Configuration parameters
DOMAIN_ID=${1:-0}
LOCALHOST_ONLY=${2:-0}

print_info "Configuring ROS2 for remote access..."
print_info "  Domain ID: $DOMAIN_ID"
print_info "  Localhost only: $LOCALHOST_ONLY"

# Create ROS2 configuration directory
ROS2_CONFIG_DIR="$HOME/.ros2_network_config"
mkdir -p "$ROS2_CONFIG_DIR"

# Create environment setup script
ENV_SCRIPT="$ROS2_CONFIG_DIR/setup_env.sh"
cat > "$ENV_SCRIPT" << EOF
#!/bin/bash
# ROS2 Network Environment Configuration
# Source this file to configure ROS2 for remote access

# ROS2 Domain ID (0-101, default: 0)
export ROS_DOMAIN_ID=$DOMAIN_ID

# Allow remote access (0 = allow, 1 = localhost only)
export ROS_LOCALHOST_ONLY=$LOCALHOST_ONLY

# DDS implementation (cyclonedds or fastrtps)
export RMW_IMPLEMENTATION=rmw_cyclonedds_cpp

# Cyclone DDS configuration file
export CYCLONEDDS_URI=file://$ROS2_CONFIG_DIR/cyclonedds.xml

echo "ROS2 network environment configured:"
echo "  ROS_DOMAIN_ID=$DOMAIN_ID"
echo "  ROS_LOCALHOST_ONLY=$LOCALHOST_ONLY"
echo "  RMW_IMPLEMENTATION=\$RMW_IMPLEMENTATION"
EOF

chmod +x "$ENV_SCRIPT"
print_info "Created environment setup script: $ENV_SCRIPT"

# Create Cyclone DDS configuration
CYCLONE_CONFIG="$ROS2_CONFIG_DIR/cyclonedds.xml"
cat > "$CYCLONE_CONFIG" << 'EOF'
<?xml version="1.0" encoding="UTF-8" ?>
<CycloneDDS xmlns="https://cdds.io/config" xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance" xsi:schemaLocation="https://cdds.io/config https://raw.githubusercontent.com/eclipse-cyclonedds/cyclonedds/master/etc/cyclonedds.xsd">
    <Domain id="any">
        <General>
            <NetworkInterfaceAddress>auto</NetworkInterfaceAddress>
            <AllowMulticast>true</AllowMulticast>
            <MaxMessageSize>65500B</MaxMessageSize>
        </General>
        <Discovery>
            <ParticipantIndex>auto</ParticipantIndex>
            <MaxAutoParticipantIndex>100</MaxAutoParticipantIndex>
            <SPDPMulticastAddress>239.255.0.1</SPDPMulticastAddress>
            <SPDPInterval>30s</SPDPInterval>
        </Discovery>
        <Tracing>
            <Verbosity>warning</Verbosity>
            <OutputFile>stdout</OutputFile>
        </Tracing>
    </Domain>
</CycloneDDS>
EOF

print_info "Created Cyclone DDS configuration: $CYCLONE_CONFIG"

# Create FastDDS configuration (alternative)
FASTDDS_CONFIG="$ROS2_CONFIG_DIR/fastdds.xml"
cat > "$FASTDDS_CONFIG" << 'EOF'
<?xml version="1.0" encoding="UTF-8" ?>
<profiles xmlns="http://www.eprosima.com/XMLSchemas/fastRTPS_Profiles">
    <transport_descriptors>
        <transport_descriptor>
            <transport_id>UDPv4Transport</transport_id>
            <type>UDPv4</type>
        </transport_descriptor>
    </transport_descriptors>

    <participant profile_name="default_participant" is_default_profile="true">
        <rtps>
            <userTransports>
                <transport_id>UDPv4Transport</transport_id>
            </userTransports>
            <useBuiltinTransports>true</useBuiltinTransports>
        </rtps>
    </participant>
</profiles>
EOF

print_info "Created FastDDS configuration: $FASTDDS_CONFIG"

# Add to bashrc if not already present
BASHRC="$HOME/.bashrc"
if ! grep -q "ros2_network_config/setup_env.sh" "$BASHRC"; then
    echo "" >> "$BASHRC"
    echo "# ROS2 Network Configuration" >> "$BASHRC"
    echo "# Uncomment the following line to enable ROS2 remote access" >> "$BASHRC"
    echo "# source $ENV_SCRIPT" >> "$BASHRC"
    print_info "Added configuration to $BASHRC (commented out)"
    print_warning "To enable, uncomment the line in $BASHRC or run: source $ENV_SCRIPT"
else
    print_info "Configuration already present in $BASHRC"
fi

# Check and configure firewall
print_info "Checking firewall configuration..."

if command -v ufw &> /dev/null; then
    print_info "UFW firewall detected"
    print_warning "To allow ROS2 traffic, run the following commands:"
    echo "  sudo ufw allow 7400:7500/udp comment 'ROS2 DDS'"
    echo "  sudo ufw allow 7400:7500/tcp comment 'ROS2 DDS'"
elif command -v firewall-cmd &> /dev/null; then
    print_info "firewalld detected"
    print_warning "To allow ROS2 traffic, run the following commands:"
    echo "  sudo firewall-cmd --permanent --add-port=7400-7500/udp"
    echo "  sudo firewall-cmd --permanent --add-port=7400-7500/tcp"
    echo "  sudo firewall-cmd --reload"
else
    print_warning "No firewall detected or firewall not recognized"
fi

# Create quick test script
TEST_SCRIPT="$ROS2_CONFIG_DIR/test_network.sh"
cat > "$TEST_SCRIPT" << 'EOF'
#!/bin/bash
# Quick test script for ROS2 network configuration

source ~/.ros2_network_config/setup_env.sh

echo "Testing ROS2 network configuration..."
echo ""
echo "Environment variables:"
echo "  ROS_DOMAIN_ID=$ROS_DOMAIN_ID"
echo "  ROS_LOCALHOST_ONLY=$ROS_LOCALHOST_ONLY"
echo "  RMW_IMPLEMENTATION=$RMW_IMPLEMENTATION"
echo ""
echo "Listing ROS2 nodes (should show nodes from all hosts in the same domain):"
ros2 node list
echo ""
echo "Listing ROS2 topics (should show topics from all hosts in the same domain):"
ros2 topic list
EOF

chmod +x "$TEST_SCRIPT"
print_info "Created test script: $TEST_SCRIPT"

echo ""
print_info "${GREEN}Configuration complete!${NC}"
echo ""
echo "Next steps:"
echo "  1. Source the environment: source $ENV_SCRIPT"
echo "  2. Configure firewall (see warnings above)"
echo "  3. Test the configuration: $TEST_SCRIPT"
echo "  4. On remote machines, run this script with the same domain ID"
echo ""
echo "To make this permanent, uncomment the line in $BASHRC"
