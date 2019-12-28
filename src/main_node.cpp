#include <iostream>
#include "zmq.hpp"
#include <string>
#include <zconf.h>
#include <vector>
#include <csignal>
#include <sstream>
#include <set>
#include <algorithm>
#include <memory>
#include <unordered_map>
#include "sf.h"

template <typename T>
std::ostream& operator << (std::ostream& os, std::vector<T> v) {
    for (const T& i : v) {
        os << i << " ";
    }
    return os;
}

struct TreeNode {
    TreeNode(int id, std::weak_ptr<TreeNode> parent)
            : id_(id), parent_(parent) {}

    int id_;
    std::weak_ptr<TreeNode> parent_;
    std::unordered_map<int, std::shared_ptr<TreeNode>> nodes_;
};

class IdIndexingTree {
public:
    IdIndexingTree() = default;
    ~IdIndexingTree() = default;

    bool Insert(int elem, int parent_id) {
        if (root_ == nullptr) {
            root_ = std::make_shared<TreeNode>(elem, std::weak_ptr<TreeNode>());
            return true;
        }
        std::vector<int> path = GetPathTo(parent_id);
        if (path.empty()) {
            return false;
        }
        path.erase(path.begin());
        std::shared_ptr<TreeNode> node = root_;
        for (int i : path) {
            if (node->nodes_.count(i) == 0) {
                throw std::logic_error("Shit happened");
            }
            node = node->nodes_[i];
        }
        node->nodes_[elem] = std::make_shared<TreeNode>(elem,node);
        return true;
    }

    bool Erase(int elem) {
        std::vector<int> path = GetPathTo(elem);
        if (path.empty()) {
            return false;
        }
        path.erase(path.begin());
        std::shared_ptr<TreeNode> node = root_;
        for (int i : path) {
            if (node->nodes_.count(i) == 0) {
                throw std::logic_error("Shit happened");
            }
            node = node->nodes_[i];
        }
        if (node->parent_.lock() == nullptr) {
            root_ = nullptr;
            return true;
        }
        node = node->parent_.lock();
        node->nodes_.erase(elem);
        return true;
    }

    [[nodiscard]] std::vector<int> GetPathTo(int id) const {
        std::vector<int> v;
        if (!SearchFunc(root_, id, v)) {
            return {};
        }
        return v;
    }

    std::vector<int> GetNodes() const {
        std::vector<int> v;
        GetNodes(root_, v);
        return v;
    }

private:
    bool SearchFunc(std::shared_ptr<TreeNode> node, int id, std::vector<int>& v) const {
        if (node == nullptr) {
            return false;
        }
        if (node->id_ == id) {
            v.push_back(node->id_);
            return true;
        }
        v.push_back(node->id_);
        for (auto [child_id, child_node] : node->nodes_) {
            if (SearchFunc(child_node, id, v)) {
                return true;
            }
        }
        v.pop_back();
        return false;
    }

    void GetNodes(std::shared_ptr<TreeNode> node, std::vector<int>& v) const {
        if (node == nullptr) {
            return;
        }
        v.push_back(node->id_);
        for (auto [child_id, child_ptr] : node->nodes_) {
            GetNodes(child_ptr, v);
        }
    }

    std::shared_ptr<TreeNode> root_ = nullptr;
};


int main() {
    std::string command;
    IdIndexingTree ids;
    size_t child_pid = 0;
    int child_id = 0;
    zmq::context_t context(1);
    zmq::socket_t main_socket(context, ZMQ_REQ);
    int linger = 0;
    main_socket.setsockopt(ZMQ_SNDTIMEO, 2000);
    main_socket.setsockopt(ZMQ_LINGER, &linger, sizeof(linger));
    int port = bind_socket(main_socket);
    while (true) {
        std::cin >> command;
        if (command == "create") {
            size_t node_id, parent_id;
            std::string result;
            std::cin >> node_id >> parent_id;
            if (child_pid == 0) {
                child_pid = fork();
                if (child_pid == -1) {
                    std::cout << "Unable to create first worker node\n";
                    child_pid = 0;
                    exit(1);
                } else if (child_pid == 0) {
                    create_node(node_id, parent_id, port);
                } else {
                    parent_id = 0;
                    //ids.Insert(node_id,0);
                    child_id = node_id;
                    send_message(main_socket,"pid");
                    result = recieve_message(main_socket);
                }

            } else {
                if (!ids.GetPathTo(node_id).empty()) {
                    std::cout << "Error: Node already exists" << "\n";
                    continue;
                }
                std::vector<int> path = ids.GetPathTo(parent_id);
                if (path.empty()) {
                    std::cout << "Error: No parent node" << "\n";
                    continue;
                }
                path.erase(path.begin());
                std::ostringstream msg_stream;
                msg_stream << "create " << path.size(); //сначала путь потом ид
                for (int i : path) {
                    msg_stream << " " << i;
                }
                msg_stream << " " << node_id;
                send_message(main_socket, msg_stream.str());
                result = recieve_message(main_socket);
            }

            if (result.substr(0,2) == "Ok") {
                ids.Insert(node_id, parent_id);
            }
            std::cout << result << "\n";

        } else if (command == "remove") {
            if (child_pid == 0) {
                std::cout << "Error: No such node\n";
                continue;
            }
            size_t node_id;
            std::cin >> node_id;
            if (node_id == child_id) {
                send_message(main_socket, "kill");
                recieve_message(main_socket);
                kill(child_pid, SIGTERM);
                kill(child_pid, SIGKILL);
                child_id = 0;
                child_pid = 0;
                std::cout << "Ok\n";
                ids.Erase(node_id);
                continue;
            }
            std::vector<int> path = ids.GetPathTo(node_id);
            if (path.empty()) {
                std::cout << "Error: No such node" << "\n";
                continue;
            }
            path.erase(path.begin());
            std::ostringstream msg_stream;
            msg_stream << "remove " << path.size() - 1;
            for (int i : path) {
                msg_stream << " " << i;
            }
            send_message(main_socket, msg_stream.str());
            std::string recieved_message = recieve_message(main_socket);
            if (recieved_message.substr(0, std::min<int>(recieved_message.size(), 2)) == "Ok") {
                ids.Erase(node_id);
            }
            std::cout << recieved_message << "\n";

        } else if (command == "exec") {
            int id, n;
            std::cin >> id >> n;
            std::vector<int> path = ids.GetPathTo(id);
            if (path.empty()) {
                std::cout << "Error: No such node" << "\n";
                continue;
            }
            path.erase(path.begin());
            std::vector<int> numbers(n);
            for (int i = 0; i < n; ++i) {
                std::cin >> numbers[i];
            }
            std::ostringstream msg_stream;
            msg_stream << "exec " << path.size();
            for (int i : path) {
                msg_stream << " " << i;
            }
            msg_stream << " " << n;
            for (int i : numbers) {
                msg_stream << " " << i;
            }
            std::string test = msg_stream.str();
            send_message(main_socket, msg_stream.str());
            std::string recieved_message = recieve_message(main_socket);
            std::cout << recieved_message << "\n";

        } else if (command == "pingall") {
            if (child_pid == 0) {
                std::cout << "No nodes\n";
                continue;
            }
            send_message(main_socket,"pingall");
            std::string recieved = recieve_message(main_socket);
            std::istringstream is(recieved);
            std::vector<int> recieved_nodes;
            int elem;
            while (is >> elem) {
                recieved_nodes.push_back(elem);
            }
            std::sort(recieved_nodes.begin(), recieved_nodes.end());
            std::vector<int> all_nodes = ids.GetNodes();
            std::sort(all_nodes.begin(), all_nodes.end());
            std::cout << "Recieved nodes " << recieved_nodes << "\n";
            std::cout << "All nodes " << all_nodes << "\n";
        } else if (command == "exit") {
            break;
        }

    }

    return 0;
}