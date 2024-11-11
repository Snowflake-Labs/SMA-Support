#!/bin/bash

VERSION_PYTHON="3.11"

check_python_version() {
    if command -v python3 &>/dev/null; then
        version=$(python3 --version 2>&1 | awk '{print $2}')
        if [[ "$version" > "${VERSION_PYTHON}" || "$version" == "${VERSION_PYTHON}" ]]; then
            echo "Python $version is already installed."
            return 0
        else
            echo "Python version is below ${VERSION_PYTHON}."
            return 1
        fi
    else
        echo "Python3 is not installed."
        return 1
    fi
}

install_python_linux() {
    echo "Installing Python ${VERSION_PYTHON}+ on Linux..."
    sudo apt update
    sudo apt install -y software-properties-common
    sudo add-apt-repository -y ppa:deadsnakes/ppa
    sudo apt update
    sudo apt install -y python3.11 python3.11-venv python3.11-dev python3.11-distutils
    sudo update-alternatives --install /usr/bin/python3 python3 /usr/bin/python3.11 1
}

install_python_mac() {
    echo "Installing Python ${VERSION_PYTHON}+ on macOS..."
    if ! command -v brew &>/dev/null; then
        echo "Homebrew is not installed. Installing Homebrew first..."
        /bin/bash -c "$(curl -fsSL https://raw.githubusercontent.com/Homebrew/install/HEAD/install.sh)"
    fi
    brew update
    brew install python@3.11
    brew link python@3.11 --force --overwrite
}

os=$(uname)

if check_python_version; then
    echo "Python ${VERSION_PYTHON} or higher is already installed."
else
    if [[ "$os" == "Linux" ]]; then
        install_python_linux
    elif [[ "$os" == "Darwin" ]]; then
        install_python_mac
    else
        echo "Windows Operating system is not supported for automatic installation. Please install Python manually."
    fi

    if check_python_version; then
        echo "Python ${VERSION_PYTHON}+ has been successfully installed and set as default."
    else
        echo "Failed to install Python ${VERSION_PYTHON}+. Please check the installation process."
    fi
fi
