# lazykafka

```
░▒▓█▓▒░       ░▒▓██████▓▒░░▒▓████████▓▒░▒▓█▓▒░░▒▓█▓▒░              
░▒▓█▓▒░      ░▒▓█▓▒░░▒▓█▓▒░      ░▒▓█▓▒░▒▓█▓▒░░▒▓█▓▒░              
░▒▓█▓▒░      ░▒▓█▓▒░░▒▓█▓▒░    ░▒▓██▓▒░░▒▓█▓▒░░▒▓█▓▒░              
░▒▓█▓▒░      ░▒▓████████▓▒░  ░▒▓██▓▒░   ░▒▓██████▓▒░               
░▒▓█▓▒░      ░▒▓█▓▒░░▒▓█▓▒░░▒▓██▓▒░       ░▒▓█▓▒░                  
░▒▓█▓▒░      ░▒▓█▓▒░░▒▓█▓▒░▒▓█▓▒░         ░▒▓█▓▒░                  
░▒▓████████▓▒░▒▓█▓▒░░▒▓█▓▒░▒▓████████▓▒░  ░▒▓█▓▒░                  
                                                                   
                                                                   
░▒▓█▓▒░░▒▓█▓▒░░▒▓██████▓▒░░▒▓████████▓▒░▒▓█▓▒░░▒▓█▓▒░░▒▓██████▓▒░  
░▒▓█▓▒░░▒▓█▓▒░▒▓█▓▒░░▒▓█▓▒░▒▓█▓▒░      ░▒▓█▓▒░░▒▓█▓▒░▒▓█▓▒░░▒▓█▓▒░ 
░▒▓█▓▒░░▒▓█▓▒░▒▓█▓▒░░▒▓█▓▒░▒▓█▓▒░      ░▒▓█▓▒░░▒▓█▓▒░▒▓█▓▒░░▒▓█▓▒░ 
░▒▓███████▓▒░░▒▓████████▓▒░▒▓██████▓▒░ ░▒▓███████▓▒░░▒▓████████▓▒░ 
░▒▓█▓▒░░▒▓█▓▒░▒▓█▓▒░░▒▓█▓▒░▒▓█▓▒░      ░▒▓█▓▒░░▒▓█▓▒░▒▓█▓▒░░▒▓█▓▒░ 
░▒▓█▓▒░░▒▓█▓▒░▒▓█▓▒░░▒▓█▓▒░▒▓█▓▒░      ░▒▓█▓▒░░▒▓█▓▒░▒▓█▓▒░░▒▓█▓▒░ 
░▒▓█▓▒░░▒▓█▓▒░▒▓█▓▒░░▒▓█▓▒░▒▓█▓▒░      ░▒▓█▓▒░░▒▓█▓▒░▒▓█▓▒░░▒▓█▓▒░
```

**Lazykafka** is a lightweight Terminal User Interface (TUI) designed to simplify the management and monitoring of Apache Kafka clusters. It provides an intuitive, keyboard-driven dashboard for visualizing broker status, inspecting topics, and tracking partition metadata without leaving the command line. The tool streamlines development workflows by allowing users to easily create or delete topics, produce test messages, and consume live data streams in real-time. Built for efficiency, Lazykafka offers a "relaxed" yet powerful alternative to complex CLI commands for developers and SREs.

**tl;dr**

 - connect to broker (connection params persisted)
 - list, add and remove topics
 - read from topics (with an option to start from beginning)
 - publish to topics

**disclaimer**

Lazykafka is designed for quick observability, not heavy lifting. We intentionally skip consumer groups and offset tracking, so you can peek into your streams without joining a group or "stealing" messages from your actual consumers. It's a transparent window into your cluster that leaves no footprints

# Installation
**Homebrew (macOS/Linux**)

    brew tap dubel/tap
    brew install lazykafka

**Linux (Debian/Ubuntu)**
Go to the releases page of this repo (https://github.com/dubel/lazykafka/releases), download the .deb file, and run:

    sudo dpkg -i lazykafka_*.deb

**Linux (RHEL/CentOS/Fedora)**
Go to the releases page, download the .rpm file, and run:

    sudo rpm -ivh lazykafka_*.rpm

**Linux (Arch Linux)**
Go to the releases page, download the .pkg.tar.zst file, and run:

    sudo pacman -U lazykafka_*.pkg.tar.zst
