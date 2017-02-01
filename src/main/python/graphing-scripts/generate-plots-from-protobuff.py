# Copyright (c) 2013, Regents of the University of California
# All rights reserved.

# Redistribution and use in source and binary forms, with or without
# modification, are permitted provided that the following conditions are met:

# Redistributions of source code must retain the above copyright notice, this
# list of conditions and the following disclaimer.  Redistributions in binary
# form must reproduce the above copyright notice, this list of conditions and the
# following disclaimer in the documentation and/or other materials provided with
# the distribution.  Neither the name of the University of California, Berkeley
# nor the names of its contributors may be used to endorse or promote products
# derived from this software without specific prior written permission.  THIS
# SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS" AND ANY
# EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE IMPLIED
# WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE ARE
# DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT HOLDER OR CONTRIBUTORS BE LIABLE
# FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL
# DAMAGES (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR
# SERVICES; LOSS OF USE, DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER
# CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY,
# OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE
# OF THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.

# This file generates a set of graphs for a simulator "experiment".
# An experiment is equivalent to the file generated from the run of a
# single Experiment object in the simulator (i.e. a parameter sweep for a
# set of workload_descs), with the added constraint that only one of
# C, L, or lambda can be varied per a single series (the simulator
# currently allows ranges to be provided for more than one of these).

import sys, os, re
from utils import *
from decimal import Decimal
import logging
import numpy as np
import matplotlib.pyplot as plt
import math
import operator
import re
import sets
from collections import defaultdict

# import cluster_simulation_protos_pb2
import imp

cluster_simulation_protos_pb2 = imp.load_source('cluster_simulation_protos_pb2', '../cluster_simulation_protos_pb2.py')

# logging.basicConfig(level=logging.DEBUG)
logging.basicConfig(level=logging.INFO)


def usage():
    print("usage: generate-plots-from-protobuff.py <output_folder> "
          "<input_protobuff_1,input_protobuff_2,...> "
          "<paper_mode: 0|1> <vary_dim: c|l|lambda> <env: any of A,B,C> [png]")
    sys.exit(1)


if len(sys.argv) < 6:
    logging.error("Not enough arguments provided.")
    usage()

paper_mode = False
output_formats = ['pdf']
output_prefix = ""
input_protobuffs = ""
try:
    output_prefix = str(sys.argv[1])
    input_protobuffs = sys.argv[2]
    if int(sys.argv[3]) == 1:
        paper_mode = True
    vary_dim = sys.argv[4]
    if vary_dim not in ['c', 'l', 'lambda']:
        logging.error("vary_dim must be c, l, or lambda!")
        sys.exit(1)
    envs_to_plot = sys.argv[5]
    if re.search("[^ABC]", envs_to_plot):
        logging.error("envs_to_plot must be any combination of a, b, and c, without spaces!")
        sys.exit(1)
    if len(sys.argv) == 7:
        if sys.argv[6] == "png":
            output_formats.append('png')
        else:
            logging.error("The only valid optional 5th argument is 'png'")
            sys.exit(1)
except:
    usage()

# set_leg_fontsize(11)

# ---------------------------------------
# Set up some general graphing variables.
set_paper_rcs()
# fig = plt.figure(figsize=(2, 1.33))
# if paper_mode:
#     set_paper_rcs()
#     fig = plt.figure(figsize=(2, 1.33))
# else:
fig = plt.figure()


def getNewColor(old):
    bw_map = {
        'b': {'marker': "D", 'dash': (None, None), 'hatch': ""},
        'g': {'marker': "v", 'dash': [10, 10], 'hatch': "-"},
        'r': {'marker': "x", 'dash': [5, 5, 2, 5], 'hatch': "\\"},
        'c': {'marker': "o", 'dash': [2, 5], 'hatch': "x"},
        'm': {'marker': None, 'dash': [5, 2, 5, 2, 5, 10], 'hatch': "+"},
        'y': {'marker': "*", 'dash': [5, 3, 1, 2, 1, 10], 'hatch': "."},
        'k': {'marker': None, 'dash': (None, None), 'hatch': "o"},  # [1,2,1,10]}
        'orange': {'marker': None, 'dash': (None, None), 'hatch': "O"},  # [1,2,1,10]}
        'navy': {'marker': None, 'dash': (None, None), 'hatch': "/"},  # [1,2,1,10]}
    }

    if old is None:
        new_color = 'r'
    elif old['color'] == 'r':
        new_color = 'b'
    elif old['color'] == 'b':
        new_color = 'y'
    elif old['color'] == 'y':
        new_color = 'g'
    elif old['color'] == 'g':
        new_color = 'c'
    elif old['color'] == 'c':
        new_color = 'm'
    elif old['color'] == 'm':
        new_color = 'orange'
    elif old['color'] == 'orange':
        new_color = 'navy'
    else:
        raise Exception("Old Color not recognized! {}".format(old['color']))

    return {"color": new_color, "line_color": new_color, "bar_color": new_color,
            "marker": bw_map[new_color]["marker"], "dash": bw_map[new_color]["dash"], "markerSize": 5,
            "markeredgewidth": 2, 'hatch': ""}

prefilled_colors_web = {'Eurecom': 'b', '10xEurecom': 'r', '5xEurecom': 'c', "SYNTH": 'y'}
colors_web = {'Eurecom': 'b', '10xEurecom': 'r', '5xEurecom': 'm', "SYNTH": 'y'}
colors_paper = {'Eurecom': 'b', '10xEurecom': 'k', '5xEurecom': 'c', "SYNTH": 'b'}
per_wl_colors = {'OmegaService': 'k',
                 'OmegaBatch': 'b'
                 }

prefilled_linestyles_web = {'Monolithic': 'D-',
                            'MonolithicApprox': 's-',
                            'MesosBatch': 'D-',
                            'MesosService': 'D:',
                            'MesosBatchApprox': 's-',
                            'MesosServiceApprox': 's:',
                            'OmegaService': 'D:',
                            'OmegaBatch': 'D-',
                            'OmegaBatchApprox': 's-',
                            'OmegaServiceApprox': 's:',
                            'Batch': 'D-',
                            'Service': 'D:'}

linestyles_web = {'Monolithic': 'x-',
                  'MonolithicApprox': 'o-',
                  'MesosBatch': 'x-',
                  'MesosService': 'x:',
                  'MesosBatchApprox': 'o-',
                  'MesosServiceApprox': 'o:',
                  'OmegaService': 'x:',
                  'OmegaBatch': 'x-',
                  'OmegaBatchApprox': 'o-',
                  'OmegaServiceApprox': 'o:',
                  'Batch': 'x-',
                  'Service': 'x:'}

linestyles_paper = {'Monolithic': '-',
                    'MonolithicApprox': '--',
                    'MesosBatch': '-',
                    'MesosService': ':',
                    'MesosBatchApprox': '--',
                    'MesosServiceApprox': '-.',
                    'OmegaService': ':',
                    'OmegaBatch': '-',
                    'OmegaBatchApprox': '--',
                    'OmegaServiceApprox': '-.',
                    'Batch': '-',
                    'Batch-MPI': '-',
                    'Service': ':',
                    'Interactive': '-',
                    '1': '^-',
                    '2': 's-',
                    '4': '+',
                    '8': 'o-',
                    '16': 'v-',
                    '32': 'x',
                    'Spark': '-',
                    'Zoe': '-',

                    'hPSJF': '--',
                    'hSJF': '--',
                    'hSRPT': '--',
                    'hFifo': '--',
                    'hHRRN': '--',
                    'SRPT': '-',
                    'PSJF': '-',
                    'SJF': '-',
                    'Fifo': '-',
                    'HRRN': '-',

                    'hPSJF2D': '--',
                    'hSJF2D': '--',
                    'hSRPT2D1': '--',
                    'hSRPT2D2': '--',
                    'hHRRN2D': '--',
                    'SRPT2D1': '-',
                    'SRPT2D2': '-',
                    'PSJF2D': '-',
                    'SJF2D': '-',
                    'HRRN2D': '-',

                    'hPSJF3D': '--',
                    'hSJF3D': '--',
                    'hSRPT3D1': '--',
                    'hSRPT3D2': '--',
                    'hHRRN3D': '--',
                    'SRPT3D1': '-',
                    'SRPT3D2': '-',
                    'PSJF3D': '-',
                    'SJF3D': '-',
                    'HRRN3D': '-',

                    'MySizeError': '--',
                    'MySize-Zoe': '--',
                    'MySize-Zoe-Preemptive': '--',

                    'Size-Zoe': '--',
                    'SRPT-Zoe': '-',
                    'SJF-Zoe': '-',
                    'MySRPT-Zoe': '-',
                    'MySJF-Zoe': '-',
                    }

dashes_paper = {'Monolithic': (None, None),
                'MonolithicApprox': (3, 3),
                'MesosBatch': (None, None),
                'MesosService': (1, 1),
                'MesosBatchApprox': (3, 3),
                'MesosServiceApprox': (4, 2),
                'OmegaBatch': (None, None),
                'OmegaService': (1, 1),
                'OmegaBatchApprox': (3, 3),
                'OmegaServiceApprox': (4, 2),
                'Batch': (None, None),
                'Batch-MPI': (4, 4),
                'Service': (1, 1),
                'Interactive': (7, 7),
                '1': (1, 1),
                '2': (None, None),
                '4': (3, 3),
                '8': (4, 2),
                '16': (2, 2),
                '32': (4, 4),
                'Spark': (None, None),
                'Zoe': (None, None),

                'hPSJF': (None, None),
                'hSJF': (None, None),
                'hSRPT': (1, 1),
                'hFifo': (3, 3),
                'hHRRN': (9, 9),
                'SRPT': (4, 4),
                'PSJF': (5, 5),
                'SJF': (5, 5),
                'Fifo': (7, 7),
                'HRRN': (8, 8),

                'hPSJF2D': (None, None),
                'hSJF2D': (None, None),
                'hSRPT2D1': (1, 1),
                'hSRPT2D2': (2, 2),
                'hHRRN2D': (9, 9),
                'SRPT2D1': (4, 4),
                'SRPT2D2': (7, 7),
                'PSJF2D': (5, 5),
                'SJF2D': (5, 5),
                'HRRN2D': (8, 8),

                'hPSJF3D': (None, None),
                'hSJF3D': (None, None),
                'hSRPT3D1': (1, 1),
                'hSRPT3D2': (2, 2),
                'hHRRN3D': (9, 9),
                'SRPT3D1': (4, 4),
                'SRPT3D2': (7, 7),
                'PSJF3D': (5, 5),
                'SJF3D': (5, 5),
                'HRRN3D': (8, 8),

                }

label_translation = {
    "Batch": "B-E",
    "Batch-MPI": "B-R",
    "Interactive": "Int",

    # "PSJF": "SJF",
    # "hPSJF": "hSJF",
    # "ePSJF": "eSJF",
    #
    # "PSJF2D": "SJF2D",
    # "hPSJF2D": "hSJF2D",
    # "ePSJF2D": "eSJF2D",
    #
    # "PSJF3D": "SJF3D",
    # "hPSJF3D": "hSJF3D",
    # "ePSJF3D": "eSJF3D",
}


if paper_mode:
    linestyles = linestyles_paper
    colors = colors_paper
    ms = 1.5
else:
    linestyles = linestyles_web
    colors = colors_web
    ms = 4

# Dicionaries to hold cell utilization indexed by by exp_env.
cell_cpu_utilization = defaultdict(list)
cell_mem_utilization = defaultdict(list)
cell_cpu_locked = defaultdict(list)
cell_mem_locked = defaultdict(list)

# Some dictionaries whose values will be dictionaries
# to make 2d dictionaries, which will be indexed by both exp_env 
# and either workoad or scheduler name.
# --
# (cellName, assignmentPolicy, workload_name) -> array of data points
# for the parameter sweep done in the experiment.
workload_queue_time_till_first = {}
workload_queue_time_till_fully = {}
workload_queue_time_till_first_90_ptile = {}
workload_queue_time_till_fully_90_ptile = {}
workload_num_jobs_unscheduled = {}
workload_num_jobs_scheduled = {}
workload_num_jobs_fully_scheduled = {}
workload_avg_job_execution_time = {}
workload_avg_job_turnaround_time = {}
workload_num_jobs_timed_out = {}
workload_avg_ramp_up_time = {}
# (cellName, assignmentPolicy, scheduler_name) -> array of data points
# for the parameter sweep done in the experiment.
sched_total_busy_fraction = {}
sched_daily_busy_fraction = {}
sched_daily_busy_fraction_err = {}
multi_sched_avg_busy_fraction = {}
multi_sched_avg_conflict_fraction = {}
# TODO(andyk): Graph retry_busy_fraction on same graph as total_busy_fraction to parallel Malte's graphs.
# sched_retry_busy_fraction = {}
sched_conflict_fraction = {}
sched_daily_conflict_fraction = {}
sched_daily_conflict_fraction_err = {}
sched_task_conflict_fraction = {}
sched_num_retried_transactions = {}
sched_num_jobs_remaining = {}
sched_failed_find_victim_attempts = {}
sched_num_jobs_timed_out = {}

workload_job_num_tasks = {}
workload_job_num_moldable_tasks = {}
workload_job_num_elastic_tasks = {}
workload_job_mem_tasks = {}
workload_job_cpu_tasks = {}
workload_job_runtime_tasks = {}
workload_job_arrival_time = {}
workload_job_inter_arrival_time = {}

workload_job_scheduled_turnaround = {}
workload_job_scheduled_turnaround_numtasks = {}
workload_job_scheduled_queue_time = {}
workload_job_scheduled_queue_time_numtasks = {}
workload_job_scheduled_execution_time = {}
workload_job_scheduled_execution_time_numtasks = {}
workload_job_unscheduled_tasks_ratio = {}

workload_job_scheduled_execution_time_per_job_id = {}

resource_utilization_cpu = {}
resource_utilization_mem = {}

pending_queue_status = {}
running_queue_status = {}

workload_job_scheduled_slowdown = {}


def moving_average_exp(interval, weight):
    N = int(weight)
    weights = np.exp(np.linspace(-1., 0., N))
    weights /= weights.sum()
    return np.convolve(weights, interval)[N-1:-N+1]

average_interval = 3600
average_threshold = average_interval * 24 * 20


def average(arr, n):
    arr = np.array(arr)
    end = n * int(len(arr)/n)
    return np.mean(arr[:end].reshape(-1, n), 1)


# Convenience wrapper to override __str__()
class ExperimentEnv:
    def __init__(self, init_exp_env):
        self.exp_env = init_exp_env
        self.cell_name = init_exp_env.cell_name
        self.workload_split_type = init_exp_env.workload_split_type
        self.is_prefilled = init_exp_env.is_prefilled
        self.run_time = init_exp_env.run_time

    def __str__(self):
        return str("%s, %s" % (self.exp_env.cell_name, self.exp_env.workload_split_type))

    # Figure out if we are varying c, l, or lambda in this experiment.
    def vary_dim(self):
        env = self.exp_env  # Make a convenient short handle.
        assert (len(env.experiment_result) > 1)
        if (env.experiment_result[0].constant_think_time !=
                env.experiment_result[1].constant_think_time):
            vary_dim = "c"
            logging.debug("Varying %s. The first two experiments' c values were %d, %d "
                          % (vary_dim,
                             env.experiment_result[0].constant_think_time,
                             env.experiment_result[1].constant_think_time))
        elif (env.experiment_result[0].per_task_think_time !=
                  env.experiment_result[1].per_task_think_time):
            vary_dim = "l"
            logging.debug("Varying %s. The first two experiments' l values were %d, %d "
                          % (vary_dim,
                             env.experiment_result[0].per_task_think_time,
                             env.experiment_result[1].per_task_think_time))
        else:
            vary_dim = "lambda"
        logging.debug("Varying %s." % vary_dim)
        return vary_dim


class Value:
    def __init__(self, init_x, init_y):
        self.x = init_x
        self.y = init_y

    def __str__(self):
        return str("%f, %f" % (self.x, self.y))


# def bt_approx(cell_name, sched_name, point, vary_dim_, tt_c, tt_l, runtime):
#     logging.debug("sched_name is %s " % sched_name)
#     assert (sched_name == "Batch" or sched_name == "Service")
#     lbd = {}
#     n = {}
#     # This function calculates an approximated scheduler busyness line given
#     # an average inter-arrival time and job size for each scheduler
#     # XXX: configure the below parameters and comment out the following
#     # line in order to
#     # 1) disable the warning, and
#     # 2) get a correct no-conflict approximation.
#     logging.warn("*********************************************\n"
#                  "YOU HAVE NOT CONFIGURED THE PARAMETERS IN THE bt_approx\n"
#                  "*********************************************\n")
#     ################################
#     # XXX EDIT BELOW HERE
#     # hard-coded SAMPLE params for cluster A
#     lbd['A'] = {"Batch": 0.1, "Service": 0.01}  # lambdas for 0: serv & 1: Batch
#     n['A'] = {"Batch": 10.0, "Service": 5.0}  # avg num tasks per job
#     # hard-coded SAMPLE params for cluster B
#     lbd['B'] = {"Batch": 0.1, "Service": 0.01}
#     n['B'] = {"Batch": 10.0, "Service": 5.0}
#     # hard-coded SAMPLE params for cluster C
#     lbd['C'] = {"Batch": 0.1, "Service": 0.01}
#     n['C'] = {"Batch": 10.0, "Service": 5.0}
#     ################################
#
#     # approximation formula
#     if vary_dim_ == 'c':
#         # busy_time = num_jobs * (per_job_think_time = C + nL) / runtime
#         return runtime * lbd[cell_name][sched_name] * \
#                (point + n[cell_name][sched_name] * float(tt_l)) / runtime
#     elif vary_dim_ == 'l':
#         return runtime * lbd[cell_name][sched_name] * \
#                (float(tt_c) + n[cell_name][sched_name] * point) / runtime


def get_mad(median, data):
    # logging.debug("in get_mad, with median %f, data: %s" % (median, " ".join([str(i) for i in data])))
    devs = [abs(x - median) for x in data]
    mad = np.median(devs)
    # logging.debug("returning mad = %f" % mad)
    return mad


def sort_labels(handles, labels):
    hl = sorted(zip(handles, labels),
                lambda x, y: cmp(int(operator.itemgetter(1)(x)), int(operator.itemgetter(1)(y))))
    handles2, labels2 = zip(*hl)
    return handles2, labels2


logging.info("Output prefix: %s" % output_prefix)
logging.info("Input file(s): %s" % input_protobuffs)

input_list = input_protobuffs.split(",")
logging.info("protobuff list: %s" % input_list)

for filename in input_list:
    logging.info("Handling file %s." % filename)
    # Read in the ExperimentResultSet.
    experiment_result_set = cluster_simulation_protos_pb2.ExperimentResultSet()
    f = open(filename, "rb")
    experiment_result_set.ParseFromString(f.read())
    f.close()

    # Loop through each experiment environment.
    experiment_name = experiment_result_set.experiment_name
    logging.info("Processing {} experiment envs with name {}".format(len(experiment_result_set.experiment_env), experiment_name))

    policy = ""
    pre = False
    for key in experiment_name.split('-'):
        if "policy" in key:
            policy = key.replace("policy_", "")
        if "zoe_preemptive" == key:
            pre = True
    if pre:
        policy += "-Pre"

    for env in experiment_result_set.experiment_env:
        exp_env = ExperimentEnv(env)  # Wrap the protobuff object to get __str__()
        logging.debug("Handling experiment env %s." % exp_env)

        # Within this environment, loop through each experiment result
        logging.debug("Processing %d experiment results." % len(env.experiment_result))

        # We're going to sort the experiment_results in case the experiments
        # in a series were run out of order.
        # Different types of experiment results require different comparators.
        def c_comparator(a, b):
            return cmp(a.constant_think_time, b.constant_think_time)


        def l_comparator(a, b):
            return cmp(a.per_task_think_time, b.per_task_think_time)


        def lambda_comparator(a, b):
            return cmp(a.avg_job_interarrival_time, b.avg_job_interarrival_time)


        sorted_exp_results = env.experiment_result
        if vary_dim == "c":
            sorted_exp_results = sorted(env.experiment_result, c_comparator)
        elif vary_dim == "l":
            sorted_exp_results = sorted(env.experiment_result, l_comparator)
        else:
            sorted_exp_results = sorted(env.experiment_result, lambda_comparator)

        logging.info("Building Workloads Specification.")
        workload_job_num_tasks = {}
        workload_job_num_moldable_tasks = {}
        workload_job_num_elastic_tasks = {}
        workload_job_mem_tasks = {}
        workload_job_cpu_tasks = {}
        workload_job_runtime_tasks = {}
        workload_job_arrival_time = {}
        workload_job_inter_arrival_time = {}
        for common_workload_stats in env.common_workload_stats:
            workload_name = common_workload_stats.workload_name
            for job_stats in common_workload_stats.job_stats:
                # Number of Tasks
                append_or_create(workload_job_num_tasks,
                                 workload_name,
                                 job_stats.num_tasks)

                # Number of Inelastic Tasks
                append_or_create(workload_job_num_moldable_tasks,
                                 workload_name,
                                 job_stats.num_inelastic)

                # Number of Elastic Tasks
                append_or_create(workload_job_num_elastic_tasks,
                                 workload_name,
                                 job_stats.num_tasks - job_stats.num_inelastic)

                # Memory per Tasks
                append_or_create(workload_job_mem_tasks,
                                 workload_name,
                                 job_stats.mem_per_task * (1024 ** 2))

                # CPU per Tasks
                append_or_create(workload_job_cpu_tasks,
                                 workload_name,
                                 job_stats.cpu_per_task)

                # Tasks Runtime
                append_or_create(workload_job_runtime_tasks,
                                 workload_name,
                                 job_stats.task_duration)

                # Arrival Time
                append_or_create(workload_job_arrival_time,
                                 workload_name,
                                 job_stats.arrival_time)

                # Inter-Arrival Time
                append_or_create(workload_job_inter_arrival_time,
                                 workload_name,
                                 job_stats.inter_arrival_time)

        for exp_result in sorted_exp_results:
            # Record the correct x val depending on which dimension is being
            # swept over in this experiment.
            # This next line is unnecessary since this value is a flag passed as an arg to the script.
            # vary_dim = exp_env.vary_dim()
            if vary_dim == "c":
                x_val = exp_result.constant_think_time
            elif vary_dim == "l":
                x_val = exp_result.per_task_think_time
            else:
                # x_val = 1.0 / exp_result.avg_job_interarrival_time
                x_val = exp_result.avg_job_interarrival_time
            logging.debug("Set x_val to %f." % x_val)

            logging.info("Building results [resources utilization]")
            # # Build result dictionaries of per exp_env stats.
            # # resource utilization
            # value = Value(x_val, exp_result.cell_state_avg_cpu_utilization)
            # cell_cpu_utilization[exp_env].append(value)
            # # logging.debug("cell_cpu_utilization[%s].append(%s)." % (exp_env, value))
            # value = Value(x_val, exp_result.cell_state_avg_mem_utilization)
            # cell_mem_utilization[exp_env].append(value)
            # # logging.debug("cell_mem_utilization[%s].append(%s)." % (exp_env, value))
            #
            # # resources locked (similar to utilization, see comments in protobuff).
            # value = Value(x_val, exp_result.cell_state_avg_cpu_locked)
            # cell_cpu_locked[exp_env].append(value)
            # # logging.debug("cell_cpu_locked[%s].append(%s)." % (exp_env, value))
            # value = Value(x_val, exp_result.cell_state_avg_mem_locked)
            # cell_mem_locked[exp_env].append(value)
            # # logging.debug("cell_mem_locked[%s].append(%s)." % (exp_env, value))

            if policy not in resource_utilization_cpu:
                resource_utilization_cpu[policy] = exp_result.resource_utilization.cpu
            if policy not in resource_utilization_mem:
                resource_utilization_mem[policy] = exp_result.resource_utilization.memory

            # i = 0
            # for cpu in exp_result.resource_utilization.cpu:
            #     resource_utilization_cpu[policy].append((i, cpu))
            #     i += 1
            # i = 0
            # for mem in exp_result.resource_utilization.memory:
            #     resource_utilization_mem[policy].append((i, mem))
            #     i += 1

            logging.info("Building results [workload]")
            # Build results dictionaries of per-workload stats.
            for wl_stat in exp_result.workload_stats:
                workload_name = wl_stat.workload_name
                for job_stats in wl_stat.job_stats:
                    # if workload_name not in workload_job_scheduled_turnaround:
                    #     workload_job_scheduled_turnaround[workload_name] = {}
                    #
                    # if workload_name not in workload_job_scheduled_turnaround_numtasks:
                    #     workload_job_scheduled_turnaround_numtasks[workload_name] = {}
                    #
                    # if workload_name not in workload_job_scheduled_execution_time:
                    #     workload_job_scheduled_execution_time[workload_name] = {}
                    #
                    # if workload_name not in workload_job_scheduled_execution_time_numtasks:
                    #     workload_job_scheduled_execution_time_numtasks[workload_name] = {}
                    #
                    # if workload_name not in workload_job_scheduled_queue_time:
                    #     workload_job_scheduled_queue_time[workload_name] = {}
                    #
                    # if workload_name not in workload_job_scheduled_queue_time_numtasks:
                    #     workload_job_scheduled_queue_time_numtasks[workload_name] = {}
                    #
                    # if workload_name not in workload_job_unscheduled_tasks_ratio:
                    #     workload_job_unscheduled_tasks_ratio[workload_name] = {}
                    #
                    # if workload_name not in workload_job_scheduled_execution_time_per_job_id:
                    #     workload_job_scheduled_execution_time_per_job_id[workload_name] = {}

                    # Turnaround
                    append_or_create_2d(workload_job_scheduled_turnaround,
                                        workload_name,
                                        policy,
                                        job_stats.turnaround)

                    # # Turnaround/num_tasks
                    # append_or_create_2d(workload_job_scheduled_turnaround_numtasks,
                    #                     workload_name,
                    #                     policy,
                    #                     job_stats.turnaround / job_stats.num_tasks)

                    # Execution Time
                    append_or_create_2d(workload_job_scheduled_execution_time,
                                        workload_name,
                                        policy,
                                        job_stats.execution_time)

                    # # Execution Time per Job id
                    # append_or_create_2d(workload_job_scheduled_execution_time_per_job_id[workload_name],
                    #                     job_stats.id,
                    #                     policy,
                    #                     job_stats.execution_time)

                    # # Execution Time/num_tasks
                    # append_or_create_2d(workload_job_scheduled_execution_time_numtasks,
                    #                     workload_name,
                    #                     policy,
                    #                     job_stats.execution_time / job_stats.num_tasks)

                    # Queue Time
                    append_or_create_2d(workload_job_scheduled_queue_time,
                                        workload_name,
                                        policy,
                                        job_stats.queue_time)

                    # # Queue Time/num_tasks
                    # append_or_create_2d(workload_job_scheduled_queue_time_numtasks,
                    #                     workload_name,
                    #                     policy,
                    #                     job_stats.queue_time / job_stats.num_tasks)
                    #
                    # # Scheduled task ratio
                    # append_or_create_2d(workload_job_unscheduled_tasks_ratio,
                    #                     workload_name,
                    #                     policy,
                    #                     1 - (
                    #                     job_stats.num_inelastic_scheduled + job_stats.num_elastic_scheduled) / float(
                    #                         job_stats.num_tasks))

                    # Slowdown Time
                    value = float(job_stats.execution_time) / float(job_stats.task_duration)
                    if value > 1.001:
                        append_or_create_2d(workload_job_scheduled_slowdown,
                                            workload_name,
                                            policy,
                                            value)

                        # # Avg. job queue times till first scheduling.
                        # value = Value(x_val, wl_stat.avg_job_queue_times_till_first_scheduled)
                        # append_or_create_2d(workload_queue_time_till_first,
                        #                     exp_env,
                        #                     workload_name,
                        #                     value)
                        # # logging.debug("workload_queue_time_till_first[%s %s].append(%s)."
                        # #               % (exp_env, wl_stat.workload_name, value))
                        #
                        # # Avg. job queue times till fully scheduling.
                        # value = Value(x_val, wl_stat.avg_job_queue_times_till_fully_scheduled)
                        # append_or_create_2d(workload_queue_time_till_fully,
                        #                     exp_env,
                        #                     workload_name,
                        #                     value)
                        # # logging.debug("workload_queue_time_till_fully[%s %s].append(%s)."
                        # #               % (exp_env, wl_stat.workload_name, value))
                        #
                        # # Avg. job ramp-up times.
                        # value = Value(x_val, wl_stat.avg_job_ramp_up_time)
                        # append_or_create_2d(workload_avg_ramp_up_time,
                        #                     exp_env,
                        #                     workload_name,
                        #                     value)
                        #
                        # # 90%tile job queue times till first scheduling.
                        # value = \
                        #     Value(x_val, wl_stat.job_queue_time_till_first_scheduled_90_percentile)
                        # append_or_create_2d(workload_queue_time_till_first_90_ptile,
                        #                     exp_env,
                        #                     workload_name,
                        #                     value)
                        # # logging.debug("workload_queue_time_till_first_90_ptile[%s %s].append(%s)."
                        # #               % (exp_env, wl_stat.workload_name, value))
                        #
                        # # 90%tile job queue times till fully scheduling.
                        # value = \
                        #     Value(x_val, wl_stat.job_queue_time_till_fully_scheduled_90_percentile)
                        # append_or_create_2d(workload_queue_time_till_fully_90_ptile,
                        #                     exp_env,
                        #                     workload_name,
                        #                     value)
                        # # logging.debug("workload_queue_time_till_fully_90_ptile[%s %s].append(%s)."
                        # #               % (exp_env, wl_stat.workload_name, value))
                        #
                        # # Num jobs that didn't schedule.
                        # value = Value(x_val, wl_stat.num_jobs - wl_stat.num_jobs_scheduled)
                        # append_or_create_2d(workload_num_jobs_unscheduled,
                        #                     exp_env,
                        #                     workload_name,
                        #                     value)
                        # # logging.debug("num_jobs_unscheduled[%s %s].append(%s)."
                        # #               % (exp_env, wl_stat.workload_name, value))
                        #
                        # # Num jobs that did schedule at least one task.
                        # value = Value(x_val, wl_stat.num_jobs_scheduled)
                        # append_or_create_2d(workload_num_jobs_scheduled,
                        #                     exp_env,
                        #                     workload_name,
                        #                     value)
                        #
                        # # Num jobs that timed out.
                        # value = Value(x_val, wl_stat.num_jobs_timed_out_scheduling)
                        # append_or_create_2d(workload_num_jobs_timed_out,
                        #                     exp_env,
                        #                     workload_name,
                        #                     value)
                        #
                        # # Num jobs that did fully schedule.
                        # value = Value(x_val, wl_stat.num_jobs_fully_scheduled)
                        # append_or_create_2d(workload_num_jobs_fully_scheduled,
                        #                     exp_env,
                        #                     workload_name,
                        #                     value)
                        #
                        # # Avg job execution time.
                        # value = Value(x_val, wl_stat.avg_job_execution_time)
                        # append_or_create_2d(workload_avg_job_execution_time,
                        #                     exp_env,
                        #                     workload_name,
                        #                     value)
                        #
                        # # Avg turnaround time.
                        # value = Value(x_val, wl_stat.avg_job_turnaround_time)
                        # append_or_create_2d(workload_avg_job_turnaround_time,
                        #                     exp_env,
                        #                     workload_name,
                        #                     value)

            # If we have multiple schedulers of the same type, track
            # some stats as an average across them, using a (counter, sum).
            # Track these stats using a map based on scheduler base name,
            # i.e., 'OmegaBatch-1' is indexed by 'OmegaBatch'. Note that this
            # depends on this naming convention being respected by the simulator.
            avg_busy_fraction = defaultdict(lambda: {"count": 0.0, "sum": 0.0})
            avg_conflict_fraction = defaultdict(lambda: {"count": 0.0, "sum": 0.0})
            # Build results dictionaries of per-scheduler stats.
            logging.info("Building results [scheduler]")
            for sched_stat in exp_result.scheduler_stats:
                #  # Generate data for the no-conflict approximation line for this
                #  # experiment_env if it is the one we are sweeping over.
                #  sweep_schedulers = []
                #  for workload_scheduler in exp_result.sweep_scheduler_workload:
                #    sweep_schedulers.append(workload_scheduler.schedulerName)
                #
                #  if sched_stat.scheduler_name in sweep_schedulers:
                #    print "trying to simplify scheduler name %s" % sched_stat.scheduler_name
                #    re_result = re.search(".*(Batch|Service)",
                #                                  sched_stat.scheduler_name)
                #    bt_approx_fraction = 0.0
                #    if re_result is not None:
                #      simple_sched_name = re_result.group(1)
                #      bt_approx_fraction = bt_approx(exp_env.cell_name,
                #                                     simple_sched_name,
                #                                     x_val,
                #                                     vary_dim,
                #                                     exp_result.constant_think_time,
                #                                     exp_result.per_task_think_time,
                #                                     exp_env.run_time)

                #    elif sched_stat.scheduler_name == "Monolithic":
                #      for sched_name in ["Batch", "Service"]:
                #        bt_approx_fraction += \
                #          bt_approx(exp_env.cell_name,
                #                    sched_name,
                #                    x_val,
                #                    vary_dim,
                #                    exp_result.constant_think_time,
                #                    exp_result.per_task_think_time,
                #                    exp_env.run_time)
                #    else:
                #      logging.error("Unrecognized scheduler name.")
                #      sys.exit(1)
                #    value = Value(x_val, bt_approx_fraction)
                #    append_or_create_2d(sched_total_busy_fraction,
                #                        exp_env,
                #                        sched_stat.scheduler_name + "Approx",
                #                        value)
                #    # logging.debug("sched_approx_busy_fraction[%s %s].append(%s)."
                #    #               % (exp_env, sched_stat.scheduler_name + "Approx", value))

                sched_name_with_policy = policy  # + "-" + sched_stat.scheduler_name
                if sched_name_with_policy not in pending_queue_status:
                    pending_queue_status[sched_name_with_policy] = sched_stat.queues_status.pending
                if sched_stat.scheduler_name not in running_queue_status:
                    running_queue_status[sched_name_with_policy] = sched_stat.queues_status.running

                    # for (i, pending) in enumerate(sched_stat.queues_status.pending):
                    #     value = Value(i, pending)
                    #     pending_queue_status[sched_name_with_policy].append(value)
                    # for (i, running) in enumerate(sched_stat.queues_status.running):
                    #     value = Value(i, running)
                    #     running_queue_status[sched_name_with_policy].append(value)

                    #     # Scheduler useful busy-time fraction.
                    #     busy_fraction = ((sched_stat.useful_busy_time +
                    #                       sched_stat.wasted_busy_time) /
                    #                      exp_env.run_time)
                    #     value = Value(x_val, busy_fraction)
                    #     append_or_create_2d(sched_total_busy_fraction,
                    #                         exp_env,
                    #                         sched_stat.scheduler_name,
                    #                         value)
                    #     # Update the running average of busytime fraction for this scheduler-type.
                    #     sched_name_root = re.match('^[^-]+', sched_stat.scheduler_name).group(0)
                    #     avg_busy_fraction[sched_name_root]["count"] += 1.0
                    #     avg_busy_fraction[sched_name_root]["sum"] += busy_fraction
                    #
                    #     # logging.debug("sched_total_busy_fraction[%s %s].append(%s)."
                    #     #               % (exp_env, sched_stat.scheduler_name, value))
                    #
                    #     # Scheduler job transaction conflict fraction.
                    #     if sched_stat.num_successful_transactions > 0:
                    #         conflict_fraction = (float(sched_stat.num_failed_transactions) /
                    #                              float(sched_stat.num_successful_transactions))
                    #     else:
                    #         conflict_fraction = 0
                    #     # logging.debug("%f / (%f + %f) = %f" %
                    #     #               (sched_stat.num_failed_transactions,
                    #     #                sched_stat.num_failed_transactions,
                    #     #                sched_stat.num_successful_transactions,
                    #     #                conflict_fraction))
                    #
                    #     value = Value(x_val, conflict_fraction)
                    #     append_or_create_2d(sched_conflict_fraction,
                    #                         exp_env,
                    #                         sched_stat.scheduler_name,
                    #                         value)
                    #     # logging.debug("sched_conflict_fraction[%s %s].append(%s)."
                    #     #               % (exp_env, sched_stat.scheduler_name, value))
                    #
                    #     # Update the running average of conflict fraction for this scheduler-type.
                    #     avg_conflict_fraction[sched_name_root]["count"] += 1.0
                    #     avg_conflict_fraction[sched_name_root]["sum"] += conflict_fraction
                    #
                    #     # Per day busy time and conflict fractions.
                    #     daily_busy_fractions = []
                    #     daily_conflict_fractions = []
                    #     for day_stats in sched_stat.per_day_stats:
                    #         # Calculate the total busy time for each of the days and then
                    #         # take median of all fo them.
                    #         run_time_for_day = exp_env.run_time - 86400 * day_stats.day_num
                    #         logging.debug("setting run_time_for_day = exp_env.run_time - 86400 * "
                    #                       "day_stats.day_num = %f - 86400 * %d = %f"
                    #                       % (exp_env.run_time, day_stats.day_num, run_time_for_day))
                    #         if run_time_for_day > 0.0:
                    #             daily_busy_fractions.append(((day_stats.useful_busy_time +
                    #                                           day_stats.wasted_busy_time) /
                    #                                          min(86400.0, run_time_for_day)))
                    #
                    #             if day_stats.num_successful_transactions > 0:
                    #                 conflict_fraction = (float(day_stats.num_failed_transactions) /
                    #                                      float(day_stats.num_successful_transactions))
                    #                 daily_conflict_fractions.append(conflict_fraction)
                    #                 logging.debug("appending daily_conflict_fraction %f." % conflict_fraction)
                    #
                    #             else:
                    #                 daily_conflict_fractions.append(0)
                    #
                    #     logging.debug("Done building daily_busy_fractions: %s"
                    #                   % " ".join([str(i) for i in daily_busy_fractions]))
                    #
                    #     # Daily busy time median.
                    #     daily_busy_time_med = np.median(daily_busy_fractions)
                    #     value = Value(x_val, daily_busy_time_med)
                    #     append_or_create_2d(sched_daily_busy_fraction,
                    #                         exp_env,
                    #                         sched_stat.scheduler_name,
                    #                         value)
                    #     # logging.debug("sched_daily_busy_fraction[%s %s].append(%s)."
                    #     #               % (exp_env, sched_stat.scheduler_name, value))
                    #     # Error Bar (MAD) for daily busy time.
                    #     value = Value(x_val, get_mad(daily_busy_time_med,
                    #                                  daily_busy_fractions))
                    #     append_or_create_2d(sched_daily_busy_fraction_err,
                    #                         exp_env,
                    #                         sched_stat.scheduler_name,
                    #                         value)
                    #     # logging.debug("sched_daily_busy_fraction_err[%s %s].append(%s)."
                    #     #               % (exp_env, sched_stat.scheduler_name, value))
                    #     # Daily conflict fraction median.
                    #     daily_conflict_fraction_med = np.median(daily_conflict_fractions)
                    #     value = Value(x_val, daily_conflict_fraction_med)
                    #     append_or_create_2d(sched_daily_conflict_fraction,
                    #                         exp_env,
                    #                         sched_stat.scheduler_name,
                    #                         value)
                    #     # logging.debug("sched_daily_conflict_fraction[%s %s].append(%s)."
                    #     #               % (exp_env, sched_stat.scheduler_name, value))
                    #     # Error Bar (MAD) for daily conflict fraction.
                    #     value = Value(x_val, get_mad(daily_conflict_fraction_med,
                    #                                  daily_conflict_fractions))
                    #     append_or_create_2d(sched_daily_conflict_fraction_err,
                    #                         exp_env,
                    #                         sched_stat.scheduler_name,
                    #                         value)
                    #     # logging.debug("sched_daily_conflict_fraction_err[%s %s].append(%s)."
                    #     #               % (exp_env, sched_stat.scheduler_name, value))
                    #
                    #     # Counts of task level transaction successes and failures.
                    #     if sched_stat.num_successful_task_transactions > 0:
                    #         task_conflict_fraction = (float(sched_stat.num_failed_task_transactions) /
                    #                                   float(sched_stat.num_failed_task_transactions +
                    #                                         sched_stat.num_successful_task_transactions))
                    #         # logging.debug("%f / (%f + %f) = %f" %
                    #         #               (sched_stat.num_failed_task_transactions,
                    #         #                sched_stat.num_failed_task_transactions,
                    #         #                sched_stat.num_successful_task_transactions,
                    #         #                conflict_fraction))
                    #
                    #         value = Value(x_val, task_conflict_fraction)
                    #         append_or_create_2d(sched_task_conflict_fraction,
                    #                             exp_env,
                    #                             sched_stat.scheduler_name,
                    #                             value)
                    #         logging.debug("sched_task_conflict_fraction[%s %s].append(%s)."
                    #                       % (exp_env, sched_stat.scheduler_name, value))
                    #
                    #     # Num retried transactions
                    #     value = Value(x_val, sched_stat.num_retried_transactions)
                    #     append_or_create_2d(sched_num_retried_transactions,
                    #                         exp_env,
                    #                         sched_stat.scheduler_name,
                    #                         value)
                    #     # logging.debug("num_retried_transactions[%s %s].append(%s)."
                    #     #               % (exp_env, sched_stat.scheduler_name, value))
                    #
                    #     # Num jobs pending at end of simulation.
                    #     value = Value(x_val, sched_stat.num_jobs_left_in_queue)
                    #     append_or_create_2d(sched_num_jobs_remaining,
                    #                         exp_env,
                    #                         sched_stat.scheduler_name,
                    #                         value)
                    #     # logging.debug("sched_num_jobs_remaining[%s %s].append(%s)."
                    #     #               % (exp_env, sched_stat.scheduler_name, value))
                    #
                    #     # Num failed find victim attempts
                    #     value = Value(x_val, sched_stat.failed_find_victim_attempts)
                    #     append_or_create_2d(sched_failed_find_victim_attempts,
                    #                         exp_env,
                    #                         sched_stat.scheduler_name,
                    #                         value)
                    #     # logging.debug("failed_find_victim_attempts[%s %s].append(%s)."
                    #     #               % (exp_env, sched_stat.scheduler_name, value))
                    #     value = Value(x_val, sched_stat.num_jobs_timed_out_scheduling)
                    #     append_or_create_2d(sched_num_jobs_timed_out,
                    #                         exp_env,
                    #                         sched_stat.scheduler_name,
                    #                         value)
                    #
                    # # Average busy time fraction across multiple schedulers
                    # for sched_name_root, stats in avg_busy_fraction.iteritems():
                    #     avg = stats["sum"] / stats["count"]
                    #     value = Value(x_val, avg)
                    #     append_or_create_2d(multi_sched_avg_busy_fraction,
                    #                         exp_env,
                    #                         sched_name_root + "-" + str(int(stats["count"])),
                    #                         value)
                    # # Average conflict fraction across multiple schedulers
                    # for sched_name_root, stats in avg_conflict_fraction.iteritems():
                    #     avg = stats["sum"] / stats["count"]
                    #     value = Value(x_val, avg)
                    #     append_or_create_2d(multi_sched_avg_conflict_fraction,
                    #                         exp_env,
                    #                         sched_name_root + "-" + str(int(stats["count"])),
                    #                         value)


def plot_1d_data_set_dict(data_set_1d_dict,
                          plot_title,
                          filename_suffix,
                          y_label,
                          y_axis_type,
                          x_axis_type=""):
    assert (y_axis_type == "0-to-1" or
            y_axis_type == "ms-to-day" or
            y_axis_type == "abs")
    logging.info("Plotting {}".format(plot_title))
    try:
        plt.clf()
        ax = fig.add_subplot(111)
        # Track a union of all x_vals which can be used to figure out
        # the x-axis for the plot we are generating.
        all_x_vals_set = sets.Set()
        for exp_env, values in data_set_1d_dict.iteritems():
            exp_env = exp_env.split("-")[0]
            # if paper_mode:
            #     cell_label = cell_to_anon(exp_env.cell_name)
            # else:
            #     cell_label = exp_env.cell_name
            #
            # # If in paper mode, skip this plot if the cell name was not
            # # passed in as argument envs_to_plot.
            # if paper_mode and not re.search(cell_label, envs_to_plot):
            #     continue
            # if exp_env.is_prefilled:
            #   # cell_label += " prefilled"
            #   # TEMPORARY: Skip prefilled to get smaller place-holder graph
            #   #            for paper draft.
            #   continue

            logging.debug("sorting x_vals")
            # x_vals = [value._1 for value in values]
            y_vals = average(values, average_interval)

            x_vals = np.arange(0, len(y_vals) * average_interval, average_interval)
            all_x_vals_set = all_x_vals_set.union(x_vals)
            logging.debug("all_x_vals_set updated, now = %s" % all_x_vals_set)
            # Rewrite zero's for the y_axis_types that will be log.
            # y_vals = [0.00001 if (value._2 == 0 and y_axis_type == "ms-to-day")
            #           else value._2 for value in values]
            logging.debug("Plotting line for %s %s." % (exp_env, plot_title))
            logging.debug("x vals: " + " ".join([str(i) for i in x_vals]))
            logging.debug("y vals: " + " ".join([str(i) for i in y_vals]))

            # if exp_env.is_prefilled:
            #     local_colors = prefilled_colors_web
            #     local_linestyles = prefilled_linestyles_web
            # else:
            # local_colors = colors
            # local_linestyles = linestyles

            if exp_env in label_translation:
                label = label_translation[exp_env]
            else:
                label = exp_env

            ax.plot(x_vals, y_vals, linestyles_paper[exp_env],
                    # color=local_colors[cell_to_anon(exp_env.cell_name)],
                    dashes=dashes_paper[exp_env],
                    label=label, markersize=ms,
                    # mec=local_colors[cell_to_anon(exp_env.cell_name)]
                    )
        if x_axis_type != "":
            vary_dim = x_axis_type
        setup_graph_details(ax, plot_title, filename_suffix, y_label, y_axis_type, all_x_vals_set, v_dim=vary_dim)
    except Exception as e:
        logging.warn(e)


# In our per-workload or per-scheduler plots, all lines
# associated with the same workload_desc
# are the same color, but have different line-types per workload_name
# or scheduler_name. In this way, we end up with a set of related lines
# for each workload_desc.
def plot_2d_data_set_dict(data_set_2d_dict,
                          plot_title,
                          filename_suffix,
                          y_label,
                          y_axis_type,
                          error_bars_data_set_2d_dict=None):
    assert (y_axis_type == "0-to-1" or
            y_axis_type == "ms-to-day" or
            y_axis_type == "abs")
    logging.info("Plotting {}".format(plot_title))
    try:
        plt.clf()
        ax = fig.add_subplot(111)
        # Track a union of all x_vals which can be used to figure out
        # the x-axis for the plot we are generating.
        all_x_vals_set = sets.Set()
        for exp_env, name_to_val_map in data_set_2d_dict.iteritems():
            if paper_mode:
                cell_label = cell_to_anon(exp_env.cell_name)
            else:
                cell_label = exp_env.cell_name

            # if exp_env.cell_name != "B":
            #      print("skipping %s" % exp_env.cell_name)
            #      continue
            #    else:
            #      print("not skipping %s" % exp_env.cell_name)

            # If in paper mode, skip this plot if the cell name was not
            # passed in as argument envs_to_plot.
            if paper_mode and not re.search(cell_label, envs_to_plot):
                logging.debug(
                    "skipping plot because cell_label %s was not passed in as envs_to_plot %s" % (
                        cell_label, envs_to_plot))
                continue
            # if exp_env.is_prefilled:
            #   # TEMPORARY: Skip prefilled to get smaller place-holder graph
            #   #            for paper draft.
            #   continue

            for wl_or_sched_name, values in name_to_val_map.iteritems():
                # Skip service schedulers.
                logging.debug("wl_or_sched_name is {}".format(wl_or_sched_name))
                # if re.search('Service', wl_or_sched_name):
                #     logging.debug("Skipping %s" % wl_or_sched_name)
                #     continue
                wl_or_sched_name_root = wl_or_sched_name
                result = re.search('^[^-]+', wl_or_sched_name)
                if result is not None:
                    wl_or_sched_name_root = result.group(0)

                wl_or_sched_num = wl_or_sched_name
                result = re.search('[0-9]+$', wl_or_sched_name)
                if result is not None:
                    wl_or_sched_num = result.group(0)

                line_label = str(wl_or_sched_num)
                # Hacky: chop MonolithicBatch, MesosBatch, MonolithicService, etc.
                # down to "Batch" and "Service" if in paper mode.
                updated_wl_or_sched_name = wl_or_sched_name
                if paper_mode and re.search("Batch", wl_or_sched_name):
                    updated_wl_or_sched_name = "Batch"
                if paper_mode and re.search("Service", wl_or_sched_name):
                    updated_wl_or_sched_name = "Service"
                # Append scheduler or workload name unless in paper mode and
                # graphing monolithic.
                # if not (paper_mode and re.search("Monolithic", wl_or_sched_name)):
                #   line_label += " " + updated_wl_or_sched_name

                # if exp_env.is_prefilled:
                #   line_label += " prefilled"
                # Don't add an item to the legend for batch schedulers/workloads
                # in paper mode. We'll explain those in the caption.
                if paper_mode and updated_wl_or_sched_name == "Service":
                    line_label = "_nolegend_"
                # if vary_dim == "lambda":
                #     x_vals = [(1 / value.x) for value in values]
                # else:
                #     x_vals = [value.x for value in values]
                x_vals = [value.x for value in values]
                logging.debug("x_vals size: {}, values size: {}".format(len(x_vals), len(values)))
                all_x_vals_set = all_x_vals_set.union(x_vals)
                logging.debug("all_x_vals_set updated, now = %s" % all_x_vals_set)
                # Rewrite zero's for the y_axis_types that will be log.
                y_vals = [0.00001 if (value.y == 0 and y_axis_type == "ms-to-day")
                          else value.y for value in values]
                logging.debug("Plotting line for %s %s %s, line_label = %s." %
                              (exp_env, wl_or_sched_name, plot_title, line_label))
                logging.debug("x vals: " + " ".join([str(i) for i in x_vals]))
                logging.debug("y vals: " + " ".join([str(i) for i in y_vals]))
                if exp_env.is_prefilled:
                    local_colors = prefilled_colors_web
                    local_linestyles = prefilled_linestyles_web
                else:
                    local_colors = colors
                    local_linestyles = linestyles

                if error_bars_data_set_2d_dict is None:
                    ax.plot(x_vals, y_vals, linestyles_paper[wl_or_sched_num],
                            dashes=dashes_paper[wl_or_sched_num],
                            color=local_colors[cell_to_anon(exp_env.cell_name)],
                            label=line_label, markersize=ms,
                            mec=local_colors[cell_to_anon(exp_env.cell_name)])
                else:
                    err_bar_vals = \
                        [i.y for i in error_bars_data_set_2d_dict[exp_env][wl_or_sched_name]]
                    logging.debug("Plotting error bars: " +
                                  " ".join([str(i) for i in err_bar_vals]))
                    ax.errorbar(x_vals, y_vals,
                                # fmt=local_linestyles[wl_or_sched_name_root],
                                dashes=dashes_paper[wl_or_sched_name_root],
                                color=local_colors[exp_env.cell_name],
                                # color=per_wl_colors[wl_or_sched_name_root],
                                label=line_label,
                                markersize=ms, capsize=1, yerr=err_bar_vals)

        logging.debug("all_x_vals_set size: {}".format(len(all_x_vals_set)))
        setup_graph_details(ax, plot_title, filename_suffix, y_label, y_axis_type, all_x_vals_set, v_dim=vary_dim)
    except Exception as e:
        logging.warn(e)


def plot_distribution_2d(data_set_2d_dict,
                         plot_title,
                         filename_suffix,
                         x_label,
                         y_axis_type):
    assert (y_axis_type == "0-to-1" or
            y_axis_type == "ms-to-day" or
            y_axis_type == "abs")
    logging.info("Plotting {}".format(plot_title))
    try:
        plt.clf()
        ax = fig.add_subplot(111)
        # Track a union of all x_vals which can be used to figure out
        # the x-axis for the plot we are generating.
        all_x_vals_set = sets.Set()
        color = None
        avg = {}
        for exp_env, name_to_job_map in data_set_2d_dict.iteritems():
            # if paper_mode:
            #     cell_label = cell_to_anon(exp_env.cell_name)
            # else:
            #     cell_label = exp_env.cell_name

            # if exp_env.cell_name != "B":
            #      print("skipping %s" % exp_env.cell_name)
            #      continue
            #    else:
            #      print("not skipping %s" % exp_env.cell_name)

            # If in paper mode, skip this plot if the cell name was not
            # passed in as argument envs_to_plot.
            # if paper_mode and not re.search(cell_label, envs_to_plot):
            #     logging.debug(
            #         "skipping plot because cell_label %s was not passed in as envs_to_plot %s" % (cell_label, envs_to_plot))
            #     continue
            # if exp_env.is_prefilled:
            #   # TEMPORARY: Skip prefilled to get smaller place-holder graph
            #   #            for paper draft.
            #   continue

            for wl_or_sched_name, values in name_to_job_map.iteritems():
                if wl_or_sched_name not in avg:
                    avg[wl_or_sched_name] = {"x": [], "y": []}
                # Skip service schedulers.
                logging.debug("wl_or_sched_name is {}".format(wl_or_sched_name))
                # if re.search('Service', wl_or_sched_name):
                #     logging.debug("Skipping %s" % wl_or_sched_name)
                #     continue

                wl_or_sched_num = wl_or_sched_name
                result = re.search('[0-9]+$', wl_or_sched_name)
                if result is not None:
                    wl_or_sched_num = result.group(0)

                line_label = str(wl_or_sched_num)
                # Hacky: chop MonolithicBatch, MesosBatch, MonolithicService, etc.
                # down to "Batch" and "Service" if in paper mode.
                updated_wl_or_sched_name = wl_or_sched_name
                if paper_mode and re.search("Batch", wl_or_sched_name):
                    updated_wl_or_sched_name = "Batch"
                if paper_mode and re.search("Service", wl_or_sched_name):
                    updated_wl_or_sched_name = "Service"
                # Append scheduler or workload name unless in paper mode and
                # graphing monolithic.
                # if not (paper_mode and re.search("Monolithic", wl_or_sched_name)):
                #   line_label += " " + updated_wl_or_sched_name

                # if exp_env.is_prefilled:
                #   line_label += " prefilled"
                # Don't add an item to the legend for batch schedulers/workloads
                # in paper mode. We'll explain those in the caption.
                if paper_mode and updated_wl_or_sched_name == "Service":
                    line_label = "_nolegend_"

                cdf = []
                x_vals = [value for value in values]
                x_vals.sort()
                for i, value in enumerate(x_vals):
                    cdf.append(i / float(len(x_vals)))

                x_vals = np.array(x_vals)
                avg[wl_or_sched_name]["x"].append(x_vals)
                all_x_vals_set = all_x_vals_set.union(x_vals)
                logging.debug("all_x_vals_set updated, now = %s" % all_x_vals_set)
                y_vals = np.array(cdf)
                avg[wl_or_sched_name]["y"].append(y_vals)
                logging.debug("Plotting line for %s %s %s, line_label = %s." %
                              (exp_env, wl_or_sched_name, plot_title, line_label))
                logging.debug("x vals: " + " ".join([str(i) for i in x_vals]))
                logging.debug("y vals: " + " ".join([str(i) for i in y_vals]))

                # if exp_env.is_prefilled:
                #     local_colors = prefilled_colors_web
                #     local_linestyles = prefilled_linestyles_web
                # else:
                #     local_colors = colors
                #     local_linestyles = linestyles

                # color = getNewColor(color)
                ax.plot(x_vals, y_vals, linestyles_paper[wl_or_sched_num],
                        dashes=dashes_paper[exp_env],
                        # color=color['color'],
                        label=line_label + "-" + exp_env, markersize=ms,
                        # mec=local_colors[cell_to_anon(exp_env.cell_name)]
                        )

        logging.debug("all_x_vals_set size: {}".format(len(all_x_vals_set)))
        setup_graph_details(ax, plot_title, filename_suffix, "", y_axis_type, all_x_vals_set, x_label=x_label)

        # plt.clf()
        # ax = fig.add_subplot(111)
        # color = None
        # for pl_name in avg:
        #     longest_x_vals = []
        #
        #     for x_vals in avg[pl_name]["x"]:
        #         if len(x_vals) > len(longest_x_vals):
        #             longest_x_vals = x_vals
        #
        #     y_vals = np.array(avg[pl_name]["y"])
        #     y_vals = y_vals.mean(axis=0)
        #
        #     logging.debug("{} Length x:{} y:{}".format(pl_name, len(longest_x_vals), len(y_vals)))
        #
        #     # color = getNewColor(color)
        #     ax.plot(longest_x_vals, y_vals, linestyles_paper[pl_name],
        #             dashes=dashes_paper[pl_name],
        #             # color=color['color'],
        #             label=pl_name, markersize=ms,
        #             # mec=local_colors[cell_to_anon(exp_env.cell_name)]
        #             )
        # setup_graph_details(ax, plot_title, filename_suffix + "-mean", "", y_axis_type, all_x_vals_set, x_label=x_label)

    except Exception as e:
        logging.warn(e)


def plot_distribution(data_set_1d_dict,
                      plot_title,
                      filename_suffix,
                      x_label,
                      y_axis_type):
    assert (y_axis_type == "0-to-1" or
            y_axis_type == "ms-to-day" or
            y_axis_type == "abs")
    logging.info("Plotting {}".format(plot_title))
    try:
        plt.clf()
        ax = fig.add_subplot(111)
        # Track a union of all x_vals which can be used to figure out
        # the x-axis for the plot we are generating.
        all_x_vals_set = sets.Set()
        color = None
        for wl_or_sched_name, values in data_set_1d_dict.iteritems():
            # Skip service schedulers.
            logging.debug("wl_or_sched_name is {}".format(wl_or_sched_name))
            # if re.search('Service', wl_or_sched_name):
            #     logging.debug("Skipping %s" % wl_or_sched_name)
            #     continue

            wl_or_sched_num = wl_or_sched_name
            result = re.search('[0-9]+$', wl_or_sched_name)
            if result is not None:
                wl_or_sched_num = result.group(0)

            line_label = str(wl_or_sched_num)
            # Hacky: chop MonolithicBatch, MesosBatch, MonolithicService, etc.
            # down to "Batch" and "Service" if in paper mode.
            updated_wl_or_sched_name = wl_or_sched_name
            if paper_mode and re.search("Batch", wl_or_sched_name):
                updated_wl_or_sched_name = "Batch"
            if paper_mode and re.search("Service", wl_or_sched_name):
                updated_wl_or_sched_name = "Service"
            # Append scheduler or workload name unless in paper mode and
            # graphing monolithic.
            # if not (paper_mode and re.search("Monolithic", wl_or_sched_name)):
            #   line_label += " " + updated_wl_or_sched_name

            # if exp_env.is_prefilled:
            #   line_label += " prefilled"
            # Don't add an item to the legend for batch schedulers/workloads
            # in paper mode. We'll explain those in the caption.
            if paper_mode and updated_wl_or_sched_name == "Service":
                line_label = "_nolegend_"

            cdf = []
            x_vals = [value for value in values]
            x_vals.sort()
            for i, value in enumerate(x_vals):
                cdf.append(i / float(len(x_vals)))

            x_vals = np.array(x_vals)
            all_x_vals_set = all_x_vals_set.union(x_vals)
            logging.debug("all_x_vals_set updated, now = %s" % all_x_vals_set)
            y_vals = np.array(cdf)
            logging.debug("Plotting line for %s %s %s, line_label = %s." %
                          (exp_env, wl_or_sched_name, plot_title, line_label))
            logging.debug("x vals: " + " ".join([str(i) for i in x_vals]))
            logging.debug("y vals: " + " ".join([str(i) for i in y_vals]))

            # if exp_env.is_prefilled:
            #     local_colors = prefilled_colors_web
            #     local_linestyles = prefilled_linestyles_web
            # else:
            #     local_colors = colors
            #     local_linestyles = linestyles

            # color = getNewColor(color)
            if line_label in label_translation:
                label = label_translation[line_label]
            else:
                label = line_label
            ax.plot(x_vals, y_vals, linestyles_paper[wl_or_sched_num],
                    dashes=dashes_paper[wl_or_sched_num],
                    # color=color['color'],
                    label=label, markersize=ms,
                    # mec=local_colors[cell_to_anon(exp_env.cell_name)]
                    )

        logging.debug("all_x_vals_set size: {}".format(len(all_x_vals_set)))
        setup_graph_details(ax, plot_title, filename_suffix, "", y_axis_type, all_x_vals_set, x_label=x_label)
    except Exception as e:
        logging.warn(e)


def sort_keys(dictionary):
    sorted_keys = []
    keys = dictionary.keys()
    if len(keys) > 0:
        keys = sorted(keys)
    for exp_env in keys:
        if exp_env not in sorted_keys:
            lb = exp_env.encode('ascii', 'ignore')
            if lb.startswith('h'):
                lb = lb[1:]
            inserted = False
            for idx in range(len(sorted_keys)):
                lb1 = sorted_keys[idx].encode('ascii', 'ignore')
                if lb1.startswith('h'):
                    lb1 = lb1[1:]

                if lb == lb1:
                    if lb == exp_env:
                        sorted_keys.insert(idx, exp_env)
                    else:
                        sorted_keys.insert(idx + 1, exp_env)
                    inserted = True
                    break
            if not inserted:
                sorted_keys.append(exp_env)
    # logging.info("{}".format(sorted_keys))
    return sorted_keys


def plot_boxplot(data_set_1d_dict,
                 plot_title,
                 filename_suffix,
                 y_label,
                 y_axis_type,
                 x_axis_type=""):
    assert (y_axis_type == "0-to-1" or
            y_axis_type == "ms-to-day" or
            y_axis_type == "abs")
    logging.info("Plotting {}".format(plot_title))
    # try:
    plt.clf()
    ax = fig.add_subplot(111)
    # Track a union of all x_vals which can be used to figure out
    # the x-axis for the plot we are generating.
    all_x_vals_set = sets.Set()
    medianprops = dict(linewidth=5, color='k')
    position = 1
    labels = []
    y_vals_set = sets.Set()
    for exp_env in sort_keys(data_set_1d_dict):
        values = data_set_1d_dict[exp_env]
        exp_env_split = exp_env.split("-")
        preemptive = False
        if len(exp_env_split) > 1 and exp_env_split[1] == "Pre":
            exp_env = exp_env_split[0]
            preemptive = True
        # else:
        #     exp_env = "No"

        if exp_env in label_translation:
            label = label_translation[exp_env]
        else:
            label = exp_env
        labels.append(label)

        x_vals = values

        logging.debug("Plotting line for %s %s." % (exp_env, plot_title))
        logging.debug("x vals: " + " ".join([str(i) for i in x_vals]))

        if len(x_vals) >= average_threshold:
            x_vals = average(x_vals, average_interval)

        color = 'gray'
        label = label.encode('ascii', 'ignore')
        if label.startswith('h'):
            color = 'w'
        if label.startswith('e'):
            color = 'r'
        if preemptive:
            color = 'gray'

        x_mav = moving_average_exp(x_vals, 500)
        y_vals_set = y_vals_set.union(x_mav)
        # x_mav = x_vals
        if len(x_mav) > 0:
            bp = ax.boxplot(x_mav, positions=[position], widths=0.8, patch_artist=True,
                            # boxprops={'edgecolor': "k", "facecolor": color},
                            # whiskerprops={'color': 'k'}, flierprops={'color': 'k'},
                            # labels=[hybrid_or_rigid],  # markersize=ms,
                            # medianprops=medianprops,
                            # showfliers=False
                            )
            for box in bp['boxes']:
                # # change outline color
                # box.set( color='#7570b3', linewidth=2)
                # change fill color
                box.set(facecolor=color)
                # box.set(hatch=hatch)
        position += 1

    if x_axis_type != "":
        vary_dim = x_axis_type
    setup_graph_details(ax, plot_title, filename_suffix, y_label, y_axis_type, labels, y_vals_set=y_vals_set, v_dim=vary_dim)
    # except Exception as e:
    #     logging.warn(e)


def plot_boxplot_2d(data_set_2d_dict,
                    plot_title,
                    filename_suffix,
                    y_label,
                    y_axis_type,
                    x_axis_type=""):
    assert (y_axis_type == "0-to-1" or
            y_axis_type == "ms-to-day" or
            y_axis_type == "abs")
    logging.info("Plotting {}".format(plot_title))
    # try:
    plt.clf()
    ax = fig.add_subplot(111)
    position = 1
    labels = []
    avg = {}
    y_vals_set = sets.Set()
    for exp_env, name_to_job_map in data_set_2d_dict.iteritems():
        for wl_or_sched_name in sort_keys(name_to_job_map):
            values = name_to_job_map[wl_or_sched_name]
            wl_or_sched_name_split = wl_or_sched_name.split("-")
            preemptive = False
            if len(wl_or_sched_name_split) > 1 and wl_or_sched_name_split[1] == "Pre":
                wl_or_sched_name = wl_or_sched_name_split[0]
                preemptive = True
            # else:
            #     wl_or_sched_name = "No"

            if wl_or_sched_name not in avg:
                avg[wl_or_sched_name] = {"x": []}
            x_vals = values
            avg[wl_or_sched_name]["x"].append(x_vals)
            logging.debug("Plotting line for %s %s %s." % (wl_or_sched_name, exp_env, plot_title))
            logging.debug("x vals: " + " ".join([str(i) for i in x_vals]))

            if len(x_vals) >= average_threshold:
                x_vals = average(x_vals, average_interval)

            if exp_env in label_translation:
                exp_env = label_translation[exp_env]
            if wl_or_sched_name in label_translation:
                wl_or_sched_name = label_translation[wl_or_sched_name]
            # labels.append(exp_env)
            labels.append(wl_or_sched_name + "-" + exp_env)

            color = 'gray'
            wl_or_sched_name = wl_or_sched_name.encode('ascii', 'ignore')
            if wl_or_sched_name.startswith('h'):
                color = 'w'
            if wl_or_sched_name.startswith('e'):
                color = 'r'
            if preemptive:
                color = 'gray'

            x_mav = moving_average_exp(x_vals, 500)
            # x_mav = x_vals
            y_vals_set = y_vals_set.union(x_mav)
            if len(x_mav) > 0:
                bp = ax.boxplot(x_mav, positions=[position], widths=0.8, patch_artist=True,
                                # boxprops={'edgecolor': "k", "facecolor": color},
                                # whiskerprops={'color': 'k'}, flierprops={'color': 'k'},
                                # labels=[hybrid_or_rigid],  # markersize=ms,
                                # medianprops=medianprops,
                                # showfliers=False
                                )
                for box in bp['boxes']:
                    # # change outline color
                    # box.set( color='#7570b3', linewidth=2)
                    # change fill color
                    box.set(facecolor=color)
                    # box.set(hatch=hatch)
            position += 1

    if x_axis_type != "":
        vary_dim = x_axis_type
    setup_graph_details(ax, plot_title, filename_suffix, y_label, y_axis_type, labels, y_vals_set=y_vals_set, v_dim=vary_dim)
    # except Exception as e:
    #     logging.warn(e)

    # plt.clf()
    # ax = fig.add_subplot(111)
    # # color = None
    # position = 1
    # for pl_name in avg:
    #     x_vals = np.array(avg[pl_name]["x"])
    #     x_vals = x_vals.mean(axis=0)
    #
    #     if len(x_vals) >= average_threshold:
    #         x_vals = average(x_vals, average_interval)
    #
    #     # x_mav = moving_average_exp(x_vals, 5)
    #     x_mav = x_vals
    #     # color = getNewColor(color)
    #     ax.boxplot(x_mav, positions=[position], widths=0.8, patch_artist=True,
    #                # boxprops={'edgecolor': "k"},  # , "facecolor": local_colors[exp_env]},
    #                # whiskerprops={'color': 'k'}, flierprops={'color': 'k'},
    #                # label=exp_env, markersize=ms,
    #                # medianprops=medianprops,
    #                # showfliers=False
    #                )
    #     position += 1
    # setup_graph_details(ax, plot_title, filename_suffix + "-mean", y_label, y_axis_type, labels, v_dim=vary_dim)


def setup_graph_details(ax, plot_title, filename_suffix, y_label, y_axis_type, x_vals_set, y_vals_set=[],  v_dim="", x_label=""):
    assert (y_axis_type == "0-to-1" or
            y_axis_type == "ms-to-day" or
            y_axis_type == "abs")

    # Paper title.
    if not paper_mode:
        plt.title(plot_title)
        leg = plt.legend(loc='upper right', bbox_to_anchor=(1.02, -0.1000), labelspacing=0, ncol=3)

    if paper_mode:
        try:
            # Set up the legend, for removing the border if in paper mode.
            logging.debug("sorting legend")
            handles, labels = ax.get_legend_handles_labels()
            handles2, labels2 = sort_labels(handles, labels)
            leg = plt.legend(handles2, labels2, loc=2, labelspacing=0, ncol=2)
            fr = leg.get_frame()
            fr.set_linewidth(0)
        except:
            logging.error("Failed to remove frame around legend, legend probably is empty.")

    # Axis labels.
    if not paper_mode:
        ax.set_ylabel(y_label)
        ax.set_xlabel(x_label)
        if v_dim == "c":
            ax.set_xlabel(u'Scheduler 1 constant processing time [sec]')
        elif v_dim == "l":
            ax.set_xlabel(u'Scheduler 1 per-task processing time [sec]')
        elif v_dim == "lambda":
            ax.set_xlabel(u'Job arrival rate to scheduler 1, lambda 1')

    # x-axis scale, limit, tics and tic labels.
    ax.set_xscale('log')
    ax.set_autoscalex_on(False)
    ax.grid(True)
    if v_dim == 'c':
        plt.xlim(xmin=0.01)
        plt.xticks((0.01, 0.1, 1, 10, 100), ('10ms', '0.1s', '1s', '10s', '100s'))
    elif v_dim == 'l':
        plt.xlim(xmin=0.001, xmax=1)
        plt.xticks((0.001, 0.01, 0.1, 1), ('1ms', '10ms', '0.1s', '1s'))
    elif v_dim == 'lambda':
        ax.set_xscale('linear')
        logging.debug("x_vals_set is %s" % x_vals_set)
        if len(x_vals_set) != 0:
            logging.debug("x_vals_set min = %s and max = %s" % (min(x_vals_set), max(x_vals_set)))
            plt.xlim([min(x_vals_set), max(x_vals_set)])
            x_vals_set = [i for i in x_vals_set if (i % 2 == 0 or i == 1)]
            plt.xticks(list(x_vals_set),
                       list([str(Decimal(x).quantize(Decimal('1'))) + "x" for x in x_vals_set]))
    elif v_dim == "timeseries":
        ax.set_xscale('linear')
        plt.xlim(xmin=0, xmax=max(x_vals_set))
        # ax.set_autoscalex_on(True)
    elif v_dim == "boxplot":
        ax.set_xscale('linear')
        ax.grid(False)
        # ax.yaxis.grid(True, which='both')
        ax.yaxis.grid(True)

        labels = []
        i = 0
        for x_val in x_vals_set:
            i += 1
            x_val = x_val.encode('ascii', 'ignore')
            if x_val.startswith('e'):
                if i % 2 == 1:
                    logging.warn("Label (and data) are not grouped correctly! {}".format(x_vals_set))
                x_val = x_val[1:]
            if x_val.startswith('h'):
                if i % 2 == 1:
                    logging.warn("Label (and data) are not grouped correctly! {}".format(x_vals_set))
                x_val = x_val[1:]

            if x_val not in labels:
                labels.append(x_val)
        # labels = x_vals_set

        x_ticks = np.arange(1, len(x_vals_set) + 1, 1)
        step = 2
        if len(labels) != len(x_vals_set):
            step = len(x_vals_set) / len(labels)
            ticks = []
            i = 0
            while i <= len(x_ticks) - step:
                # ticks.append((x_ticks[i] + x_ticks[i + 1]) / 2.0)
                # i += 2
                ticks.append(sum(x_ticks[i:i+step]) / float(step))
                # ticks.append((x_ticks[i] + x_ticks[i + 1] + x_ticks[i + 2]) / 3.0)
                i += step
            x_ticks = ticks
            logging.info("{} {}".format(step, x_ticks))
        if len(x_ticks) > 0:
            x_lim = [0, math.ceil(max(x_ticks)) + step - 1]
            # plt.xticks(x_ticks, labels)
            plt.xticks(x_ticks, labels, rotation=85)
            plt.xlim(x_lim)
    elif len(x_vals_set) != 0 and max(x_vals_set) - min(x_vals_set) < 20:
        ax.set_xscale('linear')
        ax.set_autoscalex_on(True)

    # y-axis limit, tics and tic labels.
    if y_axis_type == "0-to-1":
        logging.debug("Setting up y-axis for '0-to-1' style graph.")
        plt.ylim([0, 1.1])
        plt.yticks((0, 0.2, 0.4, 0.6, 0.8, 1.0),
                   ('0.0', '0.2', '0.4', '0.6', '0.8', '1.0'))
    elif y_axis_type == "ms-to-day":
        logging.debug("Setting up y-axis for 'ms-to-day' style graph.")
        # ax.set_yscale('symlog', linthreshy=0.001)
        ax.set_yscale('log')
        plt.ylim(ymin=0.01, ymax=24 * 3600 + 1)
        plt.yticks((0.01, 1, 60, 3600, 24 * 3600), ('10ms', '1s', '1m', '1h', '1d'))
    elif y_axis_type == "abs":
        # plt.ylim(ymin=-1)
        if len(y_vals_set) > 0 and max(y_vals_set) - min(y_vals_set) > 1000:
            ax.set_yscale('symlog')
        logging.debug("Setting up y-axis for 'abs' style graph.")
        # plt.yticks((0.01, 1, 60, 3600, 24*3600), ('10ms', '1s', '1m', '1h', '1d'))
    else:
        logging.error('y_axis_label parameter must be either "0-to-1"'
                      ', "ms-to-day", or "abs".')
        sys.exit(1)

    final_filename = os.path.join(output_prefix,
                                  '{}-{}'.format(v_dim, filename_suffix) if v_dim != "" else filename_suffix)
    logging.debug("Writing plot to %s", final_filename)
    writeout(final_filename, output_formats)


if vary_dim == "c" or vary_dim == "l":
    string_prefix = "Scheduler processing time"
else:
    string_prefix = "Job Inter-arrival time"


def generate_json(filename, dataset):
    import json
    with open(os.path.join(output_prefix, filename + '.json'), 'w') as fp:
        json.dump(dataset, fp)

workload_job_scheduled_turnaround_ratio_preemption = {}
for workload_name, workload_to_policy_map in workload_job_scheduled_turnaround.iteritems():
    logging.info("{}".format(workload_name))
    for policy in sort_keys(workload_to_policy_map):
        values = sorted(workload_to_policy_map[policy])
        policy_split = policy.split("-")
        if len(policy_split) > 1 and policy_split[1] == "Pre":
            logging.info("{} {}".format(policy, policy_split[0]))
            policy = policy_split[0]
            values_no_pre = sorted(workload_to_policy_map[policy])
            for i in range(len(values)):
                value = values_no_pre[i] / float(values[i])
                append_or_create_2d(workload_job_scheduled_turnaround_ratio_preemption,
                                    workload_name,
                                    policy,
                                    value)

# # CELL CPU UTILIZATION
# plot_1d_data_set_dict(cell_cpu_utilization,
#                       string_prefix + " vs. Avg cell CPU utilization",
#                       "avg-percent-cell-cpu-utilization",
#                       u'Avg % CPU utilization in cell',
#                       "0-to-1")
#
# # Cell MEM UTILIZATION
# plot_1d_data_set_dict(cell_mem_utilization,
#                       string_prefix + "  vs. Avg cell memory utilization",
#                       "avg-percent-cell-mem-utilization",
#                       u'Avg % memory utilization in cell',
#                       "0-to-1")
#
# # CELL CPU LOCKED
# plot_1d_data_set_dict(cell_cpu_locked,
#                       string_prefix + "  vs. Avg cell CPU locked",
#                       "avg-percent-cell-cpu-locked",
#                       u'Avg % CPU locked in cell',
#                       "0-to-1")
#
# # Cell MEM LOCKED
# plot_1d_data_set_dict(cell_mem_locked,
#                       string_prefix + "  vs. Avg cell memory locked",
#                       "avg-percent-cell-mem-locked",
#                       u'Avg % memory locked in cell',
#                       "0-to-1")
#
# # JOB QUEUE WAIT TIME PLOTS
# plot_2d_data_set_dict(workload_queue_time_till_first,
#                       string_prefix + "  vs. job wait time till first",
#                       "wait-time-first",
#                       u'Avg. wait time till first sched attempt [sec]',
#                       "ms-to-day")
#
# plot_2d_data_set_dict(workload_queue_time_till_fully,
#                       string_prefix + "  vs. job wait time till fully",
#                       "wait-time-fully",
#                       u'Avg. wait time till fully scheduled [sec]',
#                       "ms-to-day")
#
# plot_2d_data_set_dict(workload_queue_time_till_first_90_ptile,
#                       string_prefix + "  vs. 90%tile job wait time till first",
#                       "wait-time-first-90ptile",
#                       u'90%tile wait time till first scheduled [sec]',
#                       "ms-to-day")
#
# plot_2d_data_set_dict(workload_queue_time_till_fully_90_ptile,
#                       string_prefix + "  vs. 90%tile job wait time till fully",
#                       "wait-time-fully-90ptile",
#                       u'90%tile wait time till fully scheduled [sec]',
#                       "ms-to-day")
#
# plot_2d_data_set_dict(workload_num_jobs_unscheduled,
#                       string_prefix + "  vs. num jobs unscheduled",
#                       "jobs-unscheduled",
#                       u'Num jobs unscheduled',
#                       "abs")
#
# plot_2d_data_set_dict(workload_num_jobs_scheduled,
#                       string_prefix + "  vs. num jobs scheduled",
#                       "jobs-scheduled",
#                       u'Num jobs scheduled',
#                       "abs")
#
# plot_2d_data_set_dict(workload_num_jobs_timed_out,
#                       string_prefix + "  vs. num jobs ignored due to no-fit events",
#                       "num-jobs-timed-out",
#                       u'Num jobs ignored due to failed scheduling',
#                       "abs")
#
# plot_2d_data_set_dict(workload_num_jobs_fully_scheduled,
#                       string_prefix + "  vs. num jobs fully scheduled",
#                       "jobs-fully-scheduled",
#                       u'Num jobs fully scheduled',
#                       "abs")
#
# plot_2d_data_set_dict(workload_avg_job_execution_time,
#                       string_prefix + "  vs. avg jobs execution time",
#                       "avg-jobs-execution-time",
#                       u'Avg jobs execution time',
#                       "ms-to-day")
#
# plot_2d_data_set_dict(workload_avg_job_turnaround_time,
#                       string_prefix + "  vs. avg job turnaround time",
#                       "avg-jobs-turnaround-time",
#                       u'Avg jobs turnaround time',
#                       "ms-to-day")
#
# plot_2d_data_set_dict(workload_avg_ramp_up_time,
#                       string_prefix + "  vs. avg job ramp-up time",
#                       "avg-jobs-ramp-up-time",
#                       u'Avg jobs ramp-up time',
#                       "ms-to-day")
#
# # SCHEDULER BUSY TIME FRACTION PLOT
# plot_2d_data_set_dict(sched_total_busy_fraction,
#                       string_prefix + "  vs. busy time fraction",
#                       "busy-time-fraction",
#                       u'Busy time fraction',
#                       "0-to-1")
#
# # SCHEDULER CONFLICT FRACTION PLOT
# plot_2d_data_set_dict(sched_conflict_fraction,
#                       string_prefix + "  vs. conflict fraction",
#                       "conflict-fraction",
#                       u'Conflict fraction',
#                       "0-to-1")
#
# # SCHEDULER DAILY BUSY AND CONFLICT FRACTION MEDIANS
# plot_2d_data_set_dict(sched_daily_busy_fraction,
#                       string_prefix + "  vs. median(daily busy time fraction)",
#                       "daily-busy-fraction-med",
#                       u'Median(daily busy time fraction)',
#                       "0-to-1",
#                       sched_daily_busy_fraction_err)
#
# plot_2d_data_set_dict(sched_daily_conflict_fraction,
#                       string_prefix + "  vs. median(daily conflict fraction)",
#                       "daily-conflict-fraction-med",
#                       u'Median(daily conflict fraction)',
#                       "0-to-1",
#                       sched_daily_conflict_fraction_err)
#
# # AVERAGE BUSY TIME AND CONFLICT FRACTION ACROSS MULTIPLE SCHEDULERS
# plot_2d_data_set_dict(multi_sched_avg_busy_fraction,
#                       string_prefix + "  vs. avg busy time fraction",
#                       "multi-sched-avg-busy-time-fraction",
#                       u'Avg busy time fraction',
#                       "0-to-1")
#
# plot_2d_data_set_dict(multi_sched_avg_conflict_fraction,
#                       string_prefix + "  vs. avg conflict fraction",
#                       "multi-sched-avg-conflict-fraction",
#                       u'Avg conflict fraction',
#                       "0-to-1")
#
# # SCHEDULER TASK CONFLICT FRACTION PLOT
# plot_2d_data_set_dict(sched_task_conflict_fraction,
#                       string_prefix + "  vs. task conflict fraction",
#                       "task-conflict-fraction",
#                       u'Task conflict fraction',
#                       "0-to-1")
#
# plot_2d_data_set_dict(sched_num_retried_transactions,
#                       string_prefix + "  vs. retried transactions",
#                       "retried-transactions",
#                       u'Num retried transactions',
#                       "abs")
#
# plot_2d_data_set_dict(sched_num_jobs_remaining,
#                       string_prefix + "  vs. pending jobs at end of sim.",
#                       "pending-jobs-at-end",
#                       u'Num pending jobs at end of sim.',
#                       "abs")
#
# plot_2d_data_set_dict(sched_failed_find_victim_attempts,
#                       string_prefix + "  vs. failed attempts to find task for machine.",
#                       "failed-find-task-for-machine",
#                       u'Num failed attempts to find task for machine',
#                       "abs")
#
# plot_2d_data_set_dict(sched_num_jobs_timed_out,
#                       string_prefix + "  vs. num jobs ignored due to no-fit events.",
#                       "num-jobs-timed-out-scheduler",
#                       u'Num jobs ignored due to failed scheduling',
#                       "abs")

# plot_distribution(workload_job_num_tasks,
#                   "Application Services distribution",
#                   "tasks-distribution",
#                   u'Num Services',
#                   "0-to-1")
#
# plot_distribution(workload_job_num_moldable_tasks,
#                   "Core components",
#                   "tasks-inelastic-distribution",
#                   u'Num Services',
#                   "0-to-1")
#
# plot_distribution(workload_job_num_elastic_tasks,
#                   "Elastic components",
#                   "tasks-elastic-distribution",
#                   u'Num Services',
#                   "0-to-1")
#
# plot_distribution(workload_job_mem_tasks,
#                   "Components memory",
#                   "tasks-memory-distribution",
#                   u'Memory (KB)',
#                   "0-to-1")
#
# plot_distribution(workload_job_cpu_tasks,
#                   "Components CPU",
#                   "tasks-cpu-distribution",
#                   u'CPU',
#                   "0-to-1")
#
# plot_distribution(workload_job_runtime_tasks,
#                   "Estimated Runtime",
#                   "job-runtime-distribution",
#                   u'Runtime (s)',
#                   "0-to-1")
# #
# # plot_distribution(workload_job_arrival_time,
# #                   "Application Arrival Time distribution",
# #                   "job-arrival-distribution",
# #                   u'Time (s)',
# #                   "0-to-1")
# #
# plot_distribution(workload_job_inter_arrival_time,
#                   "Inter-Arrival Time",
#                   "job-inter-arrival-distribution",
#                   u'Time (s)',
#                   "0-to-1")
#
#
# plot_distribution_2d(workload_job_scheduled_turnaround,
#                      "Application Turnaround distribution",
#                      "job-turnaround-distribution",
#                      u'Runtime (s)',
#                      "0-to-1")

plot_boxplot_2d(workload_job_scheduled_turnaround,
                "Application Turnaround",
                "job-turnaround-distribution",
                u'Time (s)',
                "abs",
                "boxplot")

# plot_distribution_2d(workload_job_scheduled_execution_time,
#                      "Application Execution Time distribution",
#                      "job-execution-time-distribution",
#                      u'Runtime (s)',
#                      "0-to-1")

plot_boxplot_2d(workload_job_scheduled_execution_time,
                "Application Execution",
                "job-execution-time-distribution",
                u'Time (s)',
                "abs",
                "boxplot")

# plot_distribution_2d(workload_job_scheduled_queue_time,
#                      "Application Queue Time distribution",
#                      "job-queue-time-distribution",
#                      u'Runtime (s)',
#                      "0-to-1")

plot_boxplot_2d(workload_job_scheduled_queue_time,
                "Application Queue",
                "job-queue-time-distribution",
                u'Time (s)',
                "abs",
                "boxplot")

# plot_distribution_2d(workload_job_scheduled_slowdown,
#                      "Application Slowdown Time distribution",
#                      "job-slowdown-time-distribution",
#                      u'Runtime (s)',
#                      "0-to-1")

plot_boxplot_2d(workload_job_scheduled_slowdown,
                "Application Slowdown",
                "job-slowdown-time-distribution",
                u'Ratio',
                "abs",
                "boxplot")
if len(workload_job_scheduled_turnaround_ratio_preemption) > 0:
    plot_boxplot_2d(workload_job_scheduled_turnaround_ratio_preemption,
                    "Turnaround Ratio",
                    "job-turnaround-ratio-distribution",
                    u'Without/With preemption',
                    "abs",
                    "boxplot")

# for workload_name in workload_job_scheduled_turnaround:
#     plot_distribution(workload_job_scheduled_turnaround[workload_name],
#                       "Application Turnaround distribution",
#                       "job-turnaround-distribution-{}".format(workload_name),
#                       u'Runtime (s)',
#                       "0-to-1")
#
#     plot_distribution(workload_job_scheduled_turnaround_numtasks[workload_name],
#                       "Job Turnaround / Num-Tasks distribution",
#                       "job-turnaround-normalized-distribution-{}".format(workload_name),
#                       u'Runtime (s)',
#                       "0-to-1")
#
#
#     plot_distribution(workload_job_scheduled_execution_time_numtasks[workload_name],
#                       "Job Execution Time / Num-Tasks distribution",
#                       "job-execution-time-normalized-distribution-{}".format(workload_name),
#                       u'Runtime (s)',
#                       "0-to-1")
#
#
#     plot_distribution(workload_job_scheduled_queue_time_numtasks[workload_name],
#                       "Job Queue Time / Num-Tasks distribution",
#                       "job-queue-time-normalized-distribution-{}".format(workload_name),
#                       u'Runtime (s)',
#                       "0-to-1")
#
#     plot_distribution(workload_job_unscheduled_tasks_ratio[workload_name],
#                       "Job Tasks Unscheduled ratio distribution",
#                       "job-tasks-unscheduled-ratio-distribution-{}".format(workload_name),
#                       u'Ratio',
#                       "0-to-1")
#
# CELL CPU UTILIZATION Time Series
# plot_1d_data_set_dict(resource_utilization_cpu,
#                       "Cluster CPU allocation",
#                       "percent-cell-cpu-utilization",
#                       u'% CPU utilization in cell',
#                       "0-to-1",
#                       "timeseries")

plot_boxplot(resource_utilization_cpu,
             "Cluster CPU allocation",
             "percent-cell-cpu-utilization",
             u'% CPU',
             "0-to-1",
             "boxplot")
#
# Cell MEM UTILIZATION Time Series
# plot_1d_data_set_dict(resource_utilization_mem,
#                       "Cluster memory allocation",
#                       "percent-cell-mem-utilization",
#                       u'% memory utilization in cell',
#                       "0-to-1",
#                       "timeseries")
#
plot_boxplot(resource_utilization_mem,
             "Cluster Memory allocation",
             "percent-cell-mem-utilization",
             u'% memory',
             "0-to-1",
             "boxplot")
#
# Pending Queue Status
# plot_1d_data_set_dict(pending_queue_status,
#                       "Pending Queue Status",
#                       "pending-queue-status",
#                       u'Number Application in queue',
#                       "abs",
#                       "timeseries")

plot_boxplot(pending_queue_status,
             "Pending Queue",
             "pending-queue-status",
             u'Applications in queue',
             "abs",
             "boxplot")

# # Running Queue Status
# plot_1d_data_set_dict(running_queue_status,
#                       "Running Queue Status",
#                       "running-queue-status",
#                       u'Number Application in queue',
#                       "abs",
#                       "timeseries")

plot_boxplot(running_queue_status,
             "Running Applications",
             "running-queue-status",
             u'Applications running',
             "abs",
             "boxplot")
#
# logging.info("# Dumping datasets on JSON file...")
# logging.info("#     Execution Times per Job Id")
# generate_json('execution-times-per-job-id',
#               workload_job_scheduled_execution_time_per_job_id)
