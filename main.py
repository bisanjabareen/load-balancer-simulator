import random
import time
import heapq
import math
import sys


ARRIVAL = 'arrival'
DEPARTURE = 'departure'
class Server:
    def __init__(self, transmission_frequency, service_frequency, capacity):

        """
            arguments:
            - transmission_frequency : the frequency at which the packets arrive to the server
            - service_frequency : the frequency at which the packets are serviced
            - capacity : queue length
            - runtime : simulation runtime
        """

        self.transmission_frequency = transmission_frequency
        self.service_frequency = service_frequency
        self.is_busy = False
        self.capacity = capacity
        #self.runtime = runtime
        self.queue_size = 0
        self.queue = []
        self.A = 0
        self.B = 0
        pass
    def add_packet(self, arrival_time):
        self.queue_size += 1
        self.queue.append(arrival_time)
        return True

    def service_packet(self):

        self.queue_size -= 1
        return self.queue.pop(0)

class Simulation:
    def __init__(self, runtime, num_servers, probabilities, transmission_frequency, capacities, service_frequencies):
        self.event_list = []
        self.servers = [Server(transmission_frequency, service_frequency, capacity) for service_frequency,capacity in zip(service_frequencies, capacities)]
        self.probabilities = probabilities
        self.runtime = runtime
        self.A = 0
        self.B = 0
        self.average_service_time = 0
        self.last_timestamp = 0
        self.average_wait_time = 0
        self.wait_times = 0
        self.service_times = 0
        pass

    def select_server(self):
        """
            this function simulates a selection process in the load balancer
        """
        return random.choices(range(len(self.servers)), weights=self.probabilities)[0]

    def start_simulation(self):
        """
            entire simulation
        """
        elapsed = 0
        server_num = self.select_server()
        server = self.servers[server_num]
        heapq.heappush(self.event_list, (random.expovariate(server.transmission_frequency), ARRIVAL, server_num, None, None))
        # run for as long as there's an event (arrival events are not added once elapsed time is equal to runtime)
        while len(self.event_list) > 0:
            elapsed, event_type, server_num, curr_service_time, curr_arrival_time = heapq.heappop(self.event_list)
            server = self.servers[server_num]
            # processing arrival
            if event_type == ARRIVAL:
                #elapsed += (time.time() - elapsed)
                if server.is_busy:
                    if server.queue_size < server.capacity:
                        server.add_packet(elapsed)

                    else:
                        self.B += 1
                else:
                    service_time = random.expovariate(server.service_frequency)
                    heapq.heappush(self.event_list, (elapsed + service_time, DEPARTURE, server_num, service_time, elapsed))
                    server.is_busy = True

                # new event
                if elapsed < self.runtime:
                    server_num = self.select_server()
                    server = self.servers[server_num]
                    next_arrival = elapsed + random.expovariate(server.transmission_frequency)
                    heapq.heappush(self.event_list, (next_arrival, ARRIVAL, server_num, None, None))
            # processing departure
            elif event_type == DEPARTURE:
                self.A += 1
                self.wait_times += max(0.0, (elapsed - curr_service_time) - curr_arrival_time)
                self.service_times += curr_service_time
                if server.queue_size > 0:
                    # we prcoess the current event and schedule the next departure one because there are some more left in the queue
                    original_arrival = server.service_packet()
                    service_time = random.expovariate(server.service_frequency)
                    heapq.heappush(self.event_list, (elapsed + service_time, DEPARTURE, server_num, service_time, original_arrival))
                else:
                    # the server has no packets in the queue, it's not busy any more
                    server.is_busy = False

        self.last_timestamp = elapsed

        pass
    def print_stats(self):
        # packets that were serviced, packets that were dropped, last timestamp, average wait time, average service time
        print(f"{self.A}" +
              " " +
              f"{self.B}" +
              " " +
              f"{self.last_timestamp:.4f}" +
        " " +
              f"{(self.wait_times / self.A if self.A > 0 else 0):.4f}" +
               " " +
              f"{(self.service_times / self.A if self.A > 0 else 0):.4f}"
              )

if __name__ == "__main__":

    if len(sys.argv) < 3:
        print(
            "Usage: python script.py <simulation_duration> <num_of_servers> <server_probabilities>... <arrival_rate> <queue_capacities>... <departure_rates>...")
        sys.exit(1)

    random.seed(time.time())
    num_servers = int(sys.argv[1:][1])
    expected_args = 2 + 3 * num_servers + 1 + 1 # counting also the main.py

    if(expected_args != len(sys.argv)):
        print("error in input : wrong number of parameters")
        sys.exit(1)


    num_server_global=num_servers
    if(num_servers==0):
        print("num servers is zero")
        sys.exit(1)

    routing_weights = list(map(float, sys.argv[1:][2:2 + num_servers]))
    routing_weights_global=routing_weights
    arrival_rate = float(sys.argv[1:][2 + num_servers])
    arrival_rate_global=arrival_rate
    service_speeds = list(map(float, sys.argv[1:][3 + 2 * num_servers:3 + 3 * num_servers]))
    service_speeds_global=service_speeds
    simulation_time = int(sys.argv[1:][0])
    simulation_time_global=simulation_time
    if(simulation_time==0):
        print("simulation time is zero")
        sys.exit(1)

    sim_time=simulation_time
    sim_time_global=simulation_time_global
    queue_limits = list(map(int, sys.argv[1:][3 + num_servers:3 + 2 * num_servers]))
    queue_limits_global= queue_limits
    allgood=0
    counter=0
    for num in routing_weights:
        if( 0<= num <= 1 ):
            allgood+=1
        else:
            print("error in input: prob in index "+str(counter) +" is out of range")
            sys.exit(1)

        counter += 1

    if ( not math.isclose(sum(routing_weights), 1.0,rel_tol=1e-12, abs_tol=1e-15)):
        print("error in input : sum of probs is not 1")
        sys.exit(1)

    simulation = Simulation(simulation_time,num_servers, routing_weights, arrival_rate , queue_limits, service_speeds)
    simulation.start_simulation()
    simulation.print_stats()