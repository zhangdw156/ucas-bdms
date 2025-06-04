#include <stdio.h>
#include <string.h>
#include <limits>

#include "GraphLite.h"

#define VERTEX_CLASS_NAME(name) SSSP##name

class VERTEX_CLASS_NAME(InputFormatter): public InputFormatter {
public:
    int64_t getVertexNum() {
        unsigned long long n;
        sscanf(m_ptotal_vertex_line, "%lld", &n);
        m_total_vertex = n;
        return m_total_vertex;
    }
    int64_t getEdgeNum() {
        unsigned long long n;
        sscanf(m_ptotal_edge_line, "%lld", &n);
        m_total_edge = n;
        return m_total_edge;
    }
    int getVertexValueSize() {
        m_n_value_size = sizeof(double);
        return m_n_value_size;
    }
    int getEdgeValueSize() {
        m_e_value_size = sizeof(double);
        return m_e_value_size;
    }
    int getMessageValueSize() {
        m_m_value_size = sizeof(double);
        return m_m_value_size;
    }
    void loadGraph() {
        unsigned long long last_vertex;
        unsigned long long from;
        unsigned long long to;
        double weight = 0;

        double value = std::numeric_limits<double>::max();
        int outdegree = 0;

        const char *line = getEdgeLine();

        sscanf(line, "%lld %lld %lf", &from, &to, &weight);
        addEdge(from, to, &weight);

        last_vertex = from;
        ++outdegree;
        for (int64_t i = 1; i < m_total_edge; ++i) {
            line = getEdgeLine();
            sscanf(line, "%lld %lld %lf", &from, &to, &weight);
            if (last_vertex != from) {
                addVertex(last_vertex, &value, outdegree);
                last_vertex = from;
                outdegree = 1;
            } else {
                ++outdegree;
            }
            addEdge(from, to, &weight);
        }
        addVertex(last_vertex, &value, outdegree);
    }
};

class VERTEX_CLASS_NAME(OutputFormatter): public OutputFormatter {
public:
    void writeResult() {
        int64_t vid;
        double value;
        char s[1024];

        for (ResultIterator r_iter; !r_iter.done(); r_iter.next()) {
            r_iter.getIdValue(vid, &value);
            if (value == std::numeric_limits<double>::max()) {
                value = -1;
            }
            int n = sprintf(s, "%lld: %f\n", (unsigned long long)vid, value);
            writeNextResLine(s, n);
        }
    }
};

class VERTEX_CLASS_NAME(): public Vertex <double, double, double> {
public:
    void compute(MessageIterator* pmsgs) {
        if (getSuperstep() == 0) {
            if (getVertexId() == m_source_vertex) {
                *mutableValue() = 0;
            } else {
                *mutableValue() = std::numeric_limits<double>::max();
            }
            sendMessages();
        } else {
            double min_dist = getValue();
            for (; !pmsgs->done(); pmsgs->next()) {
                min_dist = std::min(min_dist, pmsgs->getValue());
            }
            if (min_dist < getValue()) {
                *mutableValue() = min_dist;
                sendMessages();
            }
        }
        voteToHalt();
    }

    void setSourceVertex(int64_t source) {
        m_source_vertex = source;
    }

private:
    void sendMessages() {
        double current_dist = getValue();
        if (current_dist == std::numeric_limits<double>::max()) {
            return;
        }
        OutEdgeIterator iter = getOutEdgeIterator();
        for (; !iter.done(); iter.next()) {
            sendMessageTo(iter.target(), current_dist + iter.getValue());
        }
    }

    int64_t m_source_vertex;
};

class VERTEX_CLASS_NAME(Graph): public Graph {
public:
    void init(int argc, char* argv[]) {
        setNumHosts(5);
        setHost(0, "localhost", 1411);
        setHost(1, "localhost", 1421);
        setHost(2, "localhost", 1431);
        setHost(3, "localhost", 1441);
        setHost(4, "localhost", 1451);

        if (argc < 4) {
            printf("Usage: %s <input path> <output path> <source vertex>\n", argv[0]);
            exit(1);
        }

        m_pin_path = argv[1];
        m_pout_path = argv[2];
        ((VERTEX_CLASS_NAME()*)m_pver_base)->setSourceVertex(atoll(argv[3]));
    }

    void term() {
    }
};

extern "C" Graph* create_graph() {
    Graph* pgraph = new VERTEX_CLASS_NAME(Graph);
    pgraph->m_pin_formatter = new VERTEX_CLASS_NAME(InputFormatter);
    pgraph->m_pout_formatter = new VERTEX_CLASS_NAME(OutputFormatter);
    pgraph->m_pver_base = new VERTEX_CLASS_NAME();
    return pgraph;
}

extern "C" void destroy_graph(Graph* pobject) {
    delete (VERTEX_CLASS_NAME()*)(pobject->m_pver_base);
    delete (VERTEX_CLASS_NAME(OutputFormatter)*)(pobject->m_pout_formatter);
    delete (VERTEX_CLASS_NAME(InputFormatter)*)(pobject->m_pin_formatter);
    delete (VERTEX_CLASS_NAME(Graph)*)pobject;
}