# script for downloading dataset and installing dependencies
# may need first add execute permission:
# chmod +x setup.sh

wget https://www.dropbox.com/s/64z4xtpyhhmhojp/datasets.tar.gz
tar -zvxf datasets.tar.gz
rm datasets.tar.gz
mkdir -p ../../data1/xtra
mv  datasets ../../data1/xtra

sudo apt update
sudo apt install -y cmake
# do not install latex related packages
# sudo apt install -y texlive-fonts-recommended texlive-fonts-extra
# sudo apt install -y dvipng
# sudo apt install -y font-manager
# sudo apt install -y cm-super
# sudo apt install -y python3
# sudo apt install -y python3-pip
sudo apt install -y libnuma-dev
sudo apt install -y zlib1g-dev
# sudo apt install -y python-tk
sudo apt install -y linux-tools-common
sudo apt install -y linux-tools-$(uname -r) # XXX is the kernel version of your linux, use uname -r to check it. e.g. 4.15.0-91-generic
sudo echo -1 > /proc/sys/kernel/perf_event_paranoid # if permission denied, try to run this at root user.
sudo modprobe msr
# clone flame graph maker

# clone simdprune in helper
cd ../helper
git clone https://github.com/brendangregg/FlameGraph.git