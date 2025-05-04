# Save this cell's content to page_rank.py

from mrjob.job import MRJob
from mrjob.step import MRStep
import json

class MRPageRank(MRJob):
    """
    MRJob Step 1: Initialize Graph and Assign Initial Ranks.
    Reads the parsed data (nodes and edges).
    Outputs each node with its initial PageRank and adjacency list.
    """
    N_NODES = 16
    DAMPING_FACTOR = 0.85

    # INITIALIZATION STEPS 
    def mapper_init_graph(self, _, line):
        """
        Mapper for initialization step.
        Input: One line from parsed_padgett_data.txt
               (e.g., "MEDICI\t!NODE" or "ALBIZZI\tGINORI")
        Output: Yields (node, neighbor_or_marker) pairs.
                Key: The source node (family name).
                Value: Either the target node (neighbor family name)
                       or the '!NODE' marker.
        """
        line = line.strip()

        # Split the line into parts based on the tab character
        parts = line.split('\t', 1) # Split only on the first tab

        if len(parts) == 2:
            node_key = parts[0]
            value_part = parts[1]

            # Yield the node as the key and the second part as the value
            # The reducer will group these values by node_key
            yield node_key, value_part


    def reducer_init_graph(self, node, values):
        """
        Reducer for initialization step.
        Input: node (family name), iterator of values ('!NODE' or neighbor names)
        Output: Yields (node, json_string_of_tuple) where tuple is (initial_rank, [neighbor_list])
                Using JSON string for output value ensures correct handling by subsequent mrjob steps.
        """
        neighbors = []
        node_declared = False # Flag to confirm '!NODE' was seen or links exist

        # Iterate through all values received for this node
        for value in values:
            node_declared = True
            if value != '!NODE':
                neighbors.append(value)

        # Only output if the node was actually declared in the input
        if node_declared:
            # Initialize PageRank
            initial_rank = 1.0 / self.N_NODES

            # Sort neighbors for easier debugging
            neighbors.sort()

            output_value = (initial_rank, neighbors)

            # Yield the node and the JSON encoded tuple (rank, neighbors)
            yield node, json.dumps(output_value)

    # RANK CALCULATION STEPS
    def mapper_pagerank_iter(self, node, value_str):
        """
        Mapper for a single PageRank iteration.
        Input: key=node (family name), value=JSON string "(current_rank, [neighbors])"
            (e.g., node="MEDICI", value_str="[0.0625, [\"ACCIAIUOL\", ...]]")
        Output:
            1. Yields (node, ('NODE', [neighbors])) to pass graph structure along.
            2. Yields (neighbor, ('RANK', contribution)) for each neighbor.
        """
        # Load the tuple (current_rank, neighbors) from the JSON string
        try:
            current_rank, neighbors = json.loads(value_str)
        except (json.JSONDecodeError, ValueError) as e:
            # Handle potential errors if the input isn't valid JSON or doesn't unpack correctly
            self.increment_counter('Error', 'JSONDecodeError', 1)
            return

        # 1. Emit the graph structure information for this node.
        #    The value is a tuple starting with 'NODE' marker.
        yield node, ('NODE', neighbors)

        # 2. Distribute rank contribution to neighbors.
        if neighbors: # Only distribute rank if there are outgoing links
            num_neighbors = len(neighbors)
            contribution = current_rank / num_neighbors

            # For each neighbor, emit the rank contribution.
            # The value is a tuple starting with 'RANK' marker.
            for neighbor in neighbors:
                yield neighbor, ('RANK', contribution)
        # else:
            # This is a dangling node (no outgoing links).
            # In basic PageRank, its rank "disappears" in this step,
            # but the damping factor in the reducer compensates for this lost rank globally.
            # More advanced implementations might distribute this rank differently.
            pass

    def reducer_pagerank_iter(self, node, values):
        """
        Reducer for a single PageRank iteration.
        Input: key=node (family name),
            value=iterator of tuples like ('NODE', [neighbors]) or ('RANK', contribution)
        Output: Yields (node, json_string_of_tuple) where tuple is (new_rank, [neighbor_list])
                (same format as the input to the iteration mapper)
        """
        total_received_rank = 0.0
        neighbors = []
        node_info_found = False # Flag to ensure we found the ('NODE', neighbors) tuple

        # Constants for the PageRank calculation
        N = self.N_NODES     # Get N from class variable
        damping_factor = self.DAMPING_FACTOR

        # Iterate through all values received for this node
        for value_type, data in values:
            if value_type == 'NODE':
                neighbors = data # Get the list of neighbors
                node_info_found = True
            elif value_type == 'RANK':
                total_received_rank += data # Add the received rank contribution
            # else:
                # Optional: handle unexpected value types
                # self.increment_counter('Error', 'UnknownValueTypeInReducer', 1)

        # We must have the node structure info to proceed
        if node_info_found:
            # Calculate new PageRank
            new_rank = (1 - damping_factor) / N + (damping_factor * total_received_rank)

            output_value = (new_rank, neighbors) # Keep the neighbors list

            yield node, json.dumps(output_value)
        # else:
            # This case shouldn't happen if the mapper always sends ('NODE', ...)
            # but adding a counter helps debug if it does.
            # self.increment_counter('Error', 'NodeInfoMissingInReducer', 1)

    def steps(self):
            """Define the sequence of MapReduce steps."""
            # Create a list of MRStep objects for the iterations
            iteration_steps = [
                MRStep(mapper=self.mapper_pagerank_iter,
                    reducer=self.reducer_pagerank_iter)
                for _ in range(50) # Create 10 identical iteration steps
            ]

            # Return the list starting with the initialization step,
            # followed by all the iteration steps.
            return [
                MRStep(mapper=self.mapper_init_graph,
                    reducer=self.reducer_init_graph)
            ] + iteration_steps # Concatenate the lists

# This makes the script runnable from the command line
if __name__ == '__main__':
    MRPageRank.run()
