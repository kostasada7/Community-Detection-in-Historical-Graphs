import networkx as nx
import pandas as pd
import numpy as np
import random




# Load the Orkut static edge list
def load_graph(edge_file):
    df = pd.read_csv(edge_file, sep='\t', comment='#')
    df.columns = ['source', 'target']
    #df = pd.read_csv(edge_file, sep=' ', comment='#', names=['source', 'target'])
    G = nx.Graph()
    G.add_edges_from(df.values)
    print(len(G.edges()))
    return G

def get_phase_ranges(total_ticks=528):
    phase_1_end = int((3 / 11) * total_ticks)
    phase_2_end = phase_1_end + int((4 / 11) * total_ticks)
    return phase_1_end, phase_2_end

def get_phase(t, total_ticks=528):
    phase_1_end, phase_2_end = get_phase_ranges(total_ticks)
    if t < phase_1_end:
        return 1
    elif t < phase_2_end:
        return 2
    else:
        return 3

def get_edge_lifespan(phase, current_tick, total_ticks):
    margin = random.randint(250, 500)
    max_lifespan = total_ticks - current_tick - margin
    if max_lifespan <=0:
        return random.randint(2, 15)  # force minimal acceptable lifespan if near the end

    if phase == 1:
        lifespan = 999
    elif phase == 2:
        if random.random() < 0.6:
            lifespan = int(np.random.exponential(scale=24))
        else:
            lifespan = int(np.random.normal(loc=54, scale=6))
    else:
        if random.random() < 0.6:
            lifespan = int(np.random.normal(loc=75, scale=10))  # short lived ties (6-9 months)
        else:
            lifespan = int(np.random.normal(loc=240, scale=20))  # stable ties 2 years

    return max(10, min(lifespan, max_lifespan))  # minimum lifespan = 10




def generate_birth_schedule(nodes, total_ticks):
    birth_schedule = {}
    nodes = list(nodes)
    np.random.shuffle(nodes)
    n = len(nodes)
    
    for i, node in enumerate(nodes):
        # S-curve birth time: early = slow, mid = fast, late = saturation
        normalized = i / n
        birth_tick = int(total_ticks / (1 + np.exp(-10 * (normalized - 0.5))))  # sigmoid mapping
        birth_schedule[node] = min(birth_tick, total_ticks - 1)
    
    return birth_schedule


def simulate_temporal_graph(G_static, total_ticks=528):
    active_nodes = set()
    all_edges = list(G_static.edges())  # NOT SHUFFLED
    total_edges = len(all_edges)
    birth_schedule = generate_birth_schedule(G_static.nodes(), total_ticks)
    print("Total nodes with birth times:", len(birth_schedule))

    seen_edges = set()

    # Group edges based on phase
    phase_edges = {1: [], 2: [], 3: []}
    for edge in all_edges:
        avg_birth = (birth_schedule[edge[0]] + birth_schedule[edge[1]]) // 2
        phase = get_phase(avg_birth, total_ticks)
        phase_edges[phase].append(edge)

    edges_per_tick = max(1, total_edges // total_ticks)
    print(f"Edges per tick target: {edges_per_tick}")

    # Open TXT file for tab-separated output
    with open("temporal_edges.txt", "w") as f:
        #f.write("source\ttarget\tvalid_from\tvalid_to\n")  # header

        def activate_edges():
            nonlocal seen_edges, active_nodes

            for t in range(total_ticks):
                phase = get_phase(t, total_ticks)

                # Activate nodes
                for node, birth_t in birth_schedule.items():
                    if birth_t == t:
                        active_nodes.add(node)

                # Find eligible edges
                candidates = [
                    e for e in phase_edges[phase]
                    if e[0] in active_nodes and e[1] in active_nodes and e not in seen_edges
                ]

                remaining = total_edges - len(seen_edges)
                if remaining <= 0:
                    return

                edges_this_tick = min(edges_per_tick, remaining)

                if candidates:
                    selected_edges = candidates[:edges_this_tick]  # no random
                    for edge in selected_edges:
                        lifespan = get_edge_lifespan(phase, current_tick=t, total_ticks=total_ticks)
                        death_tick = t + lifespan
                        f.write(f"{edge[0]}\t{edge[1]}\t{t}\t{death_tick}\n")
                        seen_edges.add(edge)

        while len(seen_edges) < total_edges:
            activate_edges()

        print(f"Final total activated edges: {len(seen_edges)} (Expected: {total_edges})")





if __name__ == "__main__":
    edge_file = "email.txt"
    G_static = load_graph(edge_file)
    simulate_temporal_graph(G_static)