#! /bin/python3

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

import sys

if sys.version_info[0] == 3:
    if sys.version_info[1] == 4:
        from importlib.machinery import SourceFileLoader
        cluster_simulation_protos_pb2 = SourceFileLoader("cluster_simulation_protos_pb2",
                                                         "../cluster_simulation_protos_pb2.py").load_module()
        utils = SourceFileLoader("utils", "./utils.py").load_module()
    else:
        import importlib.util
        spec = importlib.util.spec_from_file_location("cluster_simulation_protos_pb2",
                                                      "../cluster_simulation_protos_pb2.py")
        cluster_simulation_protos_pb2 = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(cluster_simulation_protos_pb2)
        spec = importlib.util.spec_from_file_location("utils", "./utils.py")
        utils = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(utils)
elif sys.version_info[0] == 2:
    import imp
    cluster_simulation_protos_pb2 = imp.load_source("cluster_simulation_protos_pb2",
                                                    "../cluster_simulation_protos_pb2.py")
    utils = imp.load_source('utils', './utils.py')

import logging
import operator
import os
import re
from collections import defaultdict
from decimal import Decimal
from filecmp import cmp

import matplotlib.pyplot as plt
import numpy as np
import sets

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

utils.set_leg_fontsize(11)

# ---------------------------------------
# Set up some general graphing variables.
if paper_mode:
    utils.set_paper_rcs()
    fig = plt.figure(figsize=(2, 1.33))
else:
    fig = plt.figure()

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
                    'Service': ':',
                    '1': '^-',
                    '2': 's-',
                    '4': '+',
                    '8': 'o-',
                    '16': 'v-',
                    '32': 'x',
                    'Spark': '-',
                    'Zoe': '-'}

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
                'Service': (1, 1),
                '1': (1, 1),
                '2': (None, None),
                '4': (3, 3),
                '8': (4, 2),
                '16': (2, 2),
                '32': (4, 4),
                'Spark': (None, None),
                'Zoe': (None, None)}

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
workload_avg_job_completion_time = {}
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
workload_job_mem_tasks = {}
workload_job_cpu_tasks = {}
workload_job_runtime_tasks = {}


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
    handles2, labels2 = list(zip(*hl))
    return handles2, labels2


output_prefix = os.path.normpath(output_prefix)
logging.info("Output prefix: %s" % output_prefix)
logging.info("Input file(s): %s" % input_protobuffs)

input_list = input_protobuffs.split(",")
logging.info("protobuff list: %s" % input_list)

for filename in input_list:
    filename = os.path.normpath(filename)
    logging.info("Handling file %s." % filename)
    # Read in the ExperimentResultSet.
    experiment_result_set = cluster_simulation_protos_pb2.ExperimentResultSet()
    f = open(filename, "rb")
    experiment_result_set.ParseFromString(f.read())
    f.close()

    # Loop through each experiment environment.
    logging.debug("Processing %d experiment envs."
                  % len(experiment_result_set.experiment_env))
    for env in experiment_result_set.experiment_env:
        exp_env = ExperimentEnv(env)  # Wrap the protobuff object to get __str__()
        logging.debug("Handling experiment env %s." % exp_env)

        # Within this environment, loop through each experiment result
        logging.debug("Processing %d experiment results." % len(env.experiment_result))


        # We're going to sort the experiment_results in case the experiments
        # in a series were run out of order.
        # Different types of experiment results require different comparators.
        # def c_comparator(a, b):
        #     return cmp(a.constant_think_time, b.constant_think_time)
        #
        #
        # def l_comparator(a, b):
        #     return cmp(a.per_task_think_time, b.per_task_think_time)
        #
        #
        # def lambda_comparator(a, b):
        #     return cmp(a.avg_job_interarrival_time, b.avg_job_interarrival_time)

        def c_comparator(item):
            return item.constant_think_time


        def l_comparator(item):
            return item.per_task_think_time


        def lambda_comparator(item):
            return item.avg_job_interarrival_time


        sorted_exp_results = env.experiment_result
        if vary_dim == "c":
            # sorted_exp_results = sorted(env.experiment_result, c_comparator)
            sorted_exp_results.sort(key=c_comparator)
        elif vary_dim == "l":
            # sorted_exp_results = sorted(env.experiment_result, l_comparator)
            sorted_exp_results.sort(key=l_comparator)
        else:
            # sorted_exp_results = sorted(env.experiment_result, lambda_comparator)
            sorted_exp_results.sort(key=lambda_comparator)

        for common_workload_stats in env.common_workload_stats:
            workload_name = common_workload_stats.workload_name
            for job_stats in common_workload_stats.job_stats:
                # Number of Tasks
                utils.append_or_create_2d(workload_job_num_tasks,
                                          exp_env,
                                          workload_name,
                                          job_stats.num_tasks)

                # Number of Tasks
                utils.append_or_create_2d(workload_job_num_moldable_tasks,
                                          exp_env,
                                          workload_name,
                                          job_stats.num_tasks_moldable)

                # Memory per Tasks
                utils.append_or_create_2d(workload_job_mem_tasks,
                                          exp_env,
                                          workload_name,
                                          job_stats.mem_per_task * (1024 ** 2))

                # CPU per Tasks
                utils.append_or_create_2d(workload_job_cpu_tasks,
                                          exp_env,
                                          workload_name,
                                          job_stats.cpu_per_task)

                # Tasks Runtime
                utils.append_or_create_2d(workload_job_runtime_tasks,
                                          exp_env,
                                          workload_name,
                                          job_stats.task_duration)

        for exp_result in sorted_exp_results:
            # Record the correct x val depending on which dimension is being
            # swept over in this experiment.
            vary_dim = exp_env.vary_dim()  # This line is unnecessary since this value
            # is a flag passed as an arg to the script.
            if vary_dim == "c":
                x_val = exp_result.constant_think_time
            elif vary_dim == "l":
                x_val = exp_result.per_task_think_time
            else:
                # x_val = 1.0 / exp_result.avg_job_interarrival_time
                x_val = exp_result.avg_job_interarrival_time
            logging.debug("Set x_val to %f." % x_val)

            # Build result dictionaries of per exp_env stats.
            # resource utilization
            value = Value(x_val, exp_result.cell_state_avg_cpu_utilization)
            cell_cpu_utilization[exp_env].append(value)
            # logging.debug("cell_cpu_utilization[%s].append(%s)." % (exp_env, value))
            value = Value(x_val, exp_result.cell_state_avg_mem_utilization)
            cell_mem_utilization[exp_env].append(value)
            # logging.debug("cell_mem_utilization[%s].append(%s)." % (exp_env, value))

            # resources locked (similar to utilization, see comments in protobuff).
            value = Value(x_val, exp_result.cell_state_avg_cpu_locked)
            cell_cpu_locked[exp_env].append(value)
            # logging.debug("cell_cpu_locked[%s].append(%s)." % (exp_env, value))
            value = Value(x_val, exp_result.cell_state_avg_mem_locked)
            cell_mem_locked[exp_env].append(value)
            # logging.debug("cell_mem_locked[%s].append(%s)." % (exp_env, value))

            # Build results dictionaries of per-workload stats.
            for wl_stat in exp_result.workload_stats:
                # Avg. job queue times till first scheduling.
                value = Value(x_val, wl_stat.avg_job_queue_times_till_first_scheduled)
                utils.append_or_create_2d(workload_queue_time_till_first,
                                          exp_env,
                                          wl_stat.workload_name,
                                          value)
                # logging.debug("workload_queue_time_till_first[%s %s].append(%s)."
                #               % (exp_env, wl_stat.workload_name, value))

                # Avg. job queue times till fully scheduling.
                value = Value(x_val, wl_stat.avg_job_queue_times_till_fully_scheduled)
                utils.append_or_create_2d(workload_queue_time_till_fully,
                                          exp_env,
                                          wl_stat.workload_name,
                                          value)
                # logging.debug("workload_queue_time_till_fully[%s %s].append(%s)."
                #               % (exp_env, wl_stat.workload_name, value))

                # Avg. job ramp-up times.
                value = Value(x_val, wl_stat.avg_job_ramp_up_time)
                utils.append_or_create_2d(workload_avg_ramp_up_time,
                                          exp_env,
                                          wl_stat.workload_name,
                                          value)

                # 90%tile job queue times till first scheduling.
                value = \
                    Value(x_val, wl_stat.job_queue_time_till_first_scheduled_90_percentile)
                utils.append_or_create_2d(workload_queue_time_till_first_90_ptile,
                                          exp_env,
                                          wl_stat.workload_name,
                                          value)
                # logging.debug("workload_queue_time_till_first_90_ptile[%s %s].append(%s)."
                #               % (exp_env, wl_stat.workload_name, value))

                # 90%tile job queue times till fully scheduling.
                value = \
                    Value(x_val, wl_stat.job_queue_time_till_fully_scheduled_90_percentile)
                utils.append_or_create_2d(workload_queue_time_till_fully_90_ptile,
                                          exp_env,
                                          wl_stat.workload_name,
                                          value)
                # logging.debug("workload_queue_time_till_fully_90_ptile[%s %s].append(%s)."
                #               % (exp_env, wl_stat.workload_name, value))

                # Num jobs that didn't schedule.
                value = Value(x_val, wl_stat.num_jobs - wl_stat.num_jobs_scheduled)
                utils.append_or_create_2d(workload_num_jobs_unscheduled,
                                          exp_env,
                                          wl_stat.workload_name,
                                          value)
                # logging.debug("num_jobs_unscheduled[%s %s].append(%s)."
                #               % (exp_env, wl_stat.workload_name, value))

                # Num jobs that did schedule at least one task.
                value = Value(x_val, wl_stat.num_jobs_scheduled)
                utils.append_or_create_2d(workload_num_jobs_scheduled,
                                          exp_env,
                                          wl_stat.workload_name,
                                          value)

                # Num jobs that timed out.
                value = Value(x_val, wl_stat.num_jobs_timed_out_scheduling)
                utils.append_or_create_2d(workload_num_jobs_timed_out,
                                          exp_env,
                                          wl_stat.workload_name,
                                          value)

                # Num jobs that did fully schedule.
                value = Value(x_val, wl_stat.num_jobs_fully_scheduled)
                utils.append_or_create_2d(workload_num_jobs_fully_scheduled,
                                          exp_env,
                                          wl_stat.workload_name,
                                          value)

                # Avg job execution time.
                value = Value(x_val, wl_stat.avg_job_execution_time)
                utils.append_or_create_2d(workload_avg_job_execution_time,
                                          exp_env,
                                          wl_stat.workload_name,
                                          value)

                # Avg completion time.
                value = Value(x_val, wl_stat.avg_job_completion_time)
                utils.append_or_create_2d(workload_avg_job_completion_time,
                                          exp_env,
                                          wl_stat.workload_name,
                                          value)

            # If we have multiple schedulers of the same type, track
            # some stats as an average across them, using a (counter, sum).
            # Track these stats using a map based on scheduler base name,
            # i.e., 'OmegaBatch-1' is indexed by 'OmegaBatch'. Note that this
            # depends on this naming convention being respected by the simulator.
            avg_busy_fraction = defaultdict(lambda: {"count": 0.0, "sum": 0.0})
            avg_conflict_fraction = defaultdict(lambda: {"count": 0.0, "sum": 0.0})
            # Build results dictionaries of per-scheduler stats.
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
                #    utils.append_or_create_2d(sched_total_busy_fraction,
                #                        exp_env,
                #                        sched_stat.scheduler_name + "Approx",
                #                        value)
                #    # logging.debug("sched_approx_busy_fraction[%s %s].append(%s)."
                #    #               % (exp_env, sched_stat.scheduler_name + "Approx", value))

                # Scheduler useful busy-time fraction.
                busy_fraction = ((sched_stat.useful_busy_time +
                                  sched_stat.wasted_busy_time) /
                                 exp_env.run_time)
                value = Value(x_val, busy_fraction)
                utils.append_or_create_2d(sched_total_busy_fraction,
                                          exp_env,
                                          sched_stat.scheduler_name,
                                          value)
                # Update the running average of busytime fraction for this scheduler-type.
                sched_name_root = re.match('^[^-]+', sched_stat.scheduler_name).group(0)
                avg_busy_fraction[sched_name_root]["count"] += 1.0
                avg_busy_fraction[sched_name_root]["sum"] += busy_fraction

                # logging.debug("sched_total_busy_fraction[%s %s].append(%s)."
                #               % (exp_env, sched_stat.scheduler_name, value))

                # Scheduler job transaction conflict fraction.
                if sched_stat.num_successful_transactions > 0:
                    conflict_fraction = (float(sched_stat.num_failed_transactions) /
                                         float(sched_stat.num_successful_transactions))
                else:
                    conflict_fraction = 0
                # logging.debug("%f / (%f + %f) = %f" %
                #               (sched_stat.num_failed_transactions,
                #                sched_stat.num_failed_transactions,
                #                sched_stat.num_successful_transactions,
                #                conflict_fraction))

                value = Value(x_val, conflict_fraction)
                utils.append_or_create_2d(sched_conflict_fraction,
                                          exp_env,
                                          sched_stat.scheduler_name,
                                          value)
                # logging.debug("sched_conflict_fraction[%s %s].append(%s)."
                #               % (exp_env, sched_stat.scheduler_name, value))

                # Update the running average of conflict fraction for this scheduler-type.
                avg_conflict_fraction[sched_name_root]["count"] += 1.0
                avg_conflict_fraction[sched_name_root]["sum"] += conflict_fraction

                # Per day busy time and conflict fractions.
                daily_busy_fractions = []
                daily_conflict_fractions = []
                for day_stats in sched_stat.per_day_stats:
                    # Calculate the total busy time for each of the days and then
                    # take median of all fo them.
                    run_time_for_day = exp_env.run_time - 86400 * day_stats.day_num
                    logging.debug("setting run_time_for_day = exp_env.run_time - 86400 * "
                                  "day_stats.day_num = %f - 86400 * %d = %f"
                                  % (exp_env.run_time, day_stats.day_num, run_time_for_day))
                    if run_time_for_day > 0.0:
                        daily_busy_fractions.append(((day_stats.useful_busy_time +
                                                      day_stats.wasted_busy_time) /
                                                     min(86400.0, run_time_for_day)))

                        if day_stats.num_successful_transactions > 0:
                            conflict_fraction = (float(day_stats.num_failed_transactions) /
                                                 float(day_stats.num_successful_transactions))
                            daily_conflict_fractions.append(conflict_fraction)
                            logging.debug("appending daily_conflict_fraction %f." % conflict_fraction)

                        else:
                            daily_conflict_fractions.append(0)

                logging.debug("Done building daily_busy_fractions: %s"
                              % " ".join([str(i) for i in daily_busy_fractions]))

                # Daily busy time median.
                daily_busy_time_med = np.median(daily_busy_fractions)
                value = Value(x_val, daily_busy_time_med)
                utils.append_or_create_2d(sched_daily_busy_fraction,
                                          exp_env,
                                          sched_stat.scheduler_name,
                                          value)
                # logging.debug("sched_daily_busy_fraction[%s %s].append(%s)."
                #               % (exp_env, sched_stat.scheduler_name, value))
                # Error Bar (MAD) for daily busy time.
                value = Value(x_val, get_mad(daily_busy_time_med,
                                             daily_busy_fractions))
                utils.append_or_create_2d(sched_daily_busy_fraction_err,
                                          exp_env,
                                          sched_stat.scheduler_name,
                                          value)
                # logging.debug("sched_daily_busy_fraction_err[%s %s].append(%s)."
                #               % (exp_env, sched_stat.scheduler_name, value))
                # Daily conflict fraction median.
                daily_conflict_fraction_med = np.median(daily_conflict_fractions)
                value = Value(x_val, daily_conflict_fraction_med)
                utils.append_or_create_2d(sched_daily_conflict_fraction,
                                          exp_env,
                                          sched_stat.scheduler_name,
                                          value)
                # logging.debug("sched_daily_conflict_fraction[%s %s].append(%s)."
                #               % (exp_env, sched_stat.scheduler_name, value))
                # Error Bar (MAD) for daily conflict fraction.
                value = Value(x_val, get_mad(daily_conflict_fraction_med,
                                             daily_conflict_fractions))
                utils.append_or_create_2d(sched_daily_conflict_fraction_err,
                                          exp_env,
                                          sched_stat.scheduler_name,
                                          value)
                # logging.debug("sched_daily_conflict_fraction_err[%s %s].append(%s)."
                #               % (exp_env, sched_stat.scheduler_name, value))

                # Counts of task level transaction successes and failures.
                if sched_stat.num_successful_task_transactions > 0:
                    task_conflict_fraction = (float(sched_stat.num_failed_task_transactions) /
                                              float(sched_stat.num_failed_task_transactions +
                                                    sched_stat.num_successful_task_transactions))
                    # logging.debug("%f / (%f + %f) = %f" %
                    #               (sched_stat.num_failed_task_transactions,
                    #                sched_stat.num_failed_task_transactions,
                    #                sched_stat.num_successful_task_transactions,
                    #                conflict_fraction))

                    value = Value(x_val, task_conflict_fraction)
                    utils.append_or_create_2d(sched_task_conflict_fraction,
                                              exp_env,
                                              sched_stat.scheduler_name,
                                              value)
                    logging.debug("sched_task_conflict_fraction[%s %s].append(%s)."
                                  % (exp_env, sched_stat.scheduler_name, value))

                # Num retried transactions
                value = Value(x_val, sched_stat.num_retried_transactions)
                utils.append_or_create_2d(sched_num_retried_transactions,
                                          exp_env,
                                          sched_stat.scheduler_name,
                                          value)
                # logging.debug("num_retried_transactions[%s %s].append(%s)."
                #               % (exp_env, sched_stat.scheduler_name, value))

                # Num jobs pending at end of simulation.
                value = Value(x_val, sched_stat.num_jobs_left_in_queue)
                utils.append_or_create_2d(sched_num_jobs_remaining,
                                          exp_env,
                                          sched_stat.scheduler_name,
                                          value)
                # logging.debug("sched_num_jobs_remaining[%s %s].append(%s)."
                #               % (exp_env, sched_stat.scheduler_name, value))

                # Num failed find victim attempts
                value = Value(x_val, sched_stat.failed_find_victim_attempts)
                utils.append_or_create_2d(sched_failed_find_victim_attempts,
                                          exp_env,
                                          sched_stat.scheduler_name,
                                          value)
                # logging.debug("failed_find_victim_attempts[%s %s].append(%s)."
                #               % (exp_env, sched_stat.scheduler_name, value))
                value = Value(x_val, sched_stat.num_jobs_timed_out_scheduling)
                utils.append_or_create_2d(sched_num_jobs_timed_out,
                                          exp_env,
                                          sched_stat.scheduler_name,
                                          value)

            # Average busy time fraction across multiple schedulers
            for sched_name_root, stats in avg_busy_fraction.items():
                avg = stats["sum"] / stats["count"]
                value = Value(x_val, avg)
                utils.append_or_create_2d(multi_sched_avg_busy_fraction,
                                          exp_env,
                                          sched_name_root + "-" + str(int(stats["count"])),
                                          value)
            # Average conflict fraction across multiple schedulers
            for sched_name_root, stats in avg_conflict_fraction.items():
                avg = stats["sum"] / stats["count"]
                value = Value(x_val, avg)
                utils.append_or_create_2d(multi_sched_avg_conflict_fraction,
                                          exp_env,
                                          sched_name_root + "-" + str(int(stats["count"])),
                                          value)


def plot_1d_data_set_dict(data_set_1d_dict,
                          plot_title,
                          filename_suffix,
                          y_label,
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
        all_x_vals_set = set()
        for exp_env, values in data_set_1d_dict.items():
            if paper_mode:
                cell_label = utils.cell_to_anon(exp_env.cell_name)
            else:
                cell_label = exp_env.cell_name

            # If in paper mode, skip this plot if the cell name was not
            # passed in as argument envs_to_plot.
            if paper_mode and not re.search(cell_label, envs_to_plot):
                continue
            # if exp_env.is_prefilled:
            #   # cell_label += " prefilled"
            #   # TEMPORARY: Skip prefilled to get smaller place-holder graph
            #   #            for paper draft.
            #   continue

            logging.debug("sorting x_vals")
            x_vals = [value.x for value in values]
            all_x_vals_set = all_x_vals_set.union(x_vals)
            logging.debug("all_x_vals_set updated, now = %s" % all_x_vals_set)
            # Rewrite zero's for the y_axis_types that will be log.
            y_vals = [0.00001 if (value.y == 0 and y_axis_type == "ms-to-day")
                      else value.y for value in values]
            logging.debug("Plotting line for %s %s." % (exp_env, plot_title))
            logging.debug("x vals: " + " ".join([str(i) for i in x_vals]))
            logging.debug("y vals: " + " ".join([str(i) for i in y_vals]))

            if exp_env.is_prefilled:
                local_colors = prefilled_colors_web
                local_linestyles = prefilled_linestyles_web
            else:
                local_colors = colors
                local_linestyles = linestyles

            ax.plot(x_vals, y_vals, 'x-',
                    color=local_colors[utils.cell_to_anon(exp_env.cell_name)],
                    label=cell_label, markersize=ms,
                    mec=local_colors[utils.cell_to_anon(exp_env.cell_name)])

        setup_graph_details(ax, plot_title, filename_suffix, y_label, y_axis_type, all_x_vals_set, v_dim=vary_dim)
    except Exception as e:
        logging.error(e)


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
        all_x_vals_set = set()
        for exp_env, name_to_val_map in data_set_2d_dict.items():
            if paper_mode:
                cell_label = utils.cell_to_anon(exp_env.cell_name)
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

            for wl_or_sched_name, values in name_to_val_map.items():
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
                            color=local_colors[utils.cell_to_anon(exp_env.cell_name)],
                            label=line_label, markersize=ms,
                            mec=local_colors[utils.cell_to_anon(exp_env.cell_name)])
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
        logging.error(e)


def plot_distribution(data_set_2d_dict,
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
        all_x_vals_set = set()
        for exp_env, name_to_job_map in data_set_2d_dict.items():
            if paper_mode:
                cell_label = utils.cell_to_anon(exp_env.cell_name)
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

            for wl_or_sched_name, values in name_to_job_map.items():
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

                if exp_env.is_prefilled:
                    local_colors = prefilled_colors_web
                    local_linestyles = prefilled_linestyles_web
                else:
                    local_colors = colors
                    local_linestyles = linestyles

                ax.plot(x_vals, y_vals, linestyles_paper[wl_or_sched_num],
                        dashes=dashes_paper[wl_or_sched_num],
                        color=local_colors[utils.cell_to_anon(exp_env.cell_name)],
                        label=line_label, markersize=ms,
                        mec=local_colors[utils.cell_to_anon(exp_env.cell_name)])

        logging.debug("all_x_vals_set size: {}".format(len(all_x_vals_set)))
        setup_graph_details(ax, plot_title, filename_suffix, "", y_axis_type, all_x_vals_set, x_label=x_label)
    except Exception as e:
        logging.error(e)


def setup_graph_details(ax, plot_title, filename_suffix, y_label, y_axis_type, x_vals_set, v_dim="", x_label=""):
    assert (y_axis_type == "0-to-1" or
            y_axis_type == "ms-to-day" or
            y_axis_type == "abs")

    # Paper title.
    if not paper_mode:
        plt.title(plot_title)
        leg = plt.legend(loc='upper right', bbox_to_anchor=(1.02, -0.1000), labelspacing=0)

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
            ax.set_xlabel('Scheduler 1 constant processing time [sec]')
        elif v_dim == "l":
            ax.set_xlabel('Scheduler 1 per-task processing time [sec]')
        elif v_dim == "lambda":
            ax.set_xlabel('Job arrival rate to scheduler 1, lambda 1')

    # x-axis scale, limit, tics and tic labels.
    ax.set_xscale('log')
    ax.set_autoscalex_on(False)
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

    # y-axis limit, tics and tic labels.
    if y_axis_type == "0-to-1":
        logging.debug("Setting up y-axis for '0-to-1' style graph.")
        plt.ylim([0, 1])
        plt.yticks((0, 0.2, 0.4, 0.6, 0.8, 1.0),
                   ('0.0', '0.2', '0.4', '0.6', '0.8', '1.0'))
    elif y_axis_type == "ms-to-day":
        logging.debug("Setting up y-axis for 'ms-to-day' style graph.")
        # ax.set_yscale('symlog', linthreshy=0.001)
        ax.set_yscale('log')
        plt.ylim(ymin=0.01, ymax=24 * 3600)
        plt.yticks((0.01, 1, 60, 3600, 24 * 3600), ('10ms', '1s', '1m', '1h', '1d'))
    elif y_axis_type == "abs":
        plt.ylim(ymin=0)
        logging.debug("Setting up y-axis for 'abs' style graph.")
        # plt.yticks((0.01, 1, 60, 3600, 24*3600), ('10ms', '1s', '1m', '1h', '1d'))
    else:
        logging.error('y_axis_label parameter must be either "0-to-1"'
                      ', "ms-to-day", or "abs".')
        sys.exit(1)

    ax.grid(True)
    final_filename = os.path.join(output_prefix,
                                  '{}-vs-{}'.format(v_dim, filename_suffix) if v_dim != "" else filename_suffix)
    logging.debug("Writing plot to %s", final_filename)
    utils.writeout(final_filename, output_formats)


if vary_dim == "c" or vary_dim == "l":
    string_prefix = "Scheduler processing time"
else:
    string_prefix = "Job Inter-arrival time"

# CELL CPU UTILIZATION
plot_1d_data_set_dict(cell_cpu_utilization,
                      string_prefix + " vs. Avg cell CPU utilization",
                      "avg-percent-cell-cpu-utilization",
                      'Avg % CPU utilization in cell',
                      "0-to-1")

# Cell MEM UTILIZATION
plot_1d_data_set_dict(cell_mem_utilization,
                      string_prefix + "  vs. Avg cell memory utilization",
                      "avg-percent-cell-mem-utilization",
                      'Avg % memory utilization in cell',
                      "0-to-1")

# CELL CPU LOCKED
plot_1d_data_set_dict(cell_cpu_locked,
                      string_prefix + "  vs. Avg cell CPU locked",
                      "avg-percent-cell-cpu-locked",
                      'Avg % CPU locked in cell',
                      "0-to-1")

# Cell MEM LOCKED
plot_1d_data_set_dict(cell_mem_locked,
                      string_prefix + "  vs. Avg cell memory locked",
                      "avg-percent-cell-mem-locked",
                      'Avg % memory locked in cell',
                      "0-to-1")
#
# JOB QUEUE WAIT TIME PLOTS
plot_2d_data_set_dict(workload_queue_time_till_first,
                      string_prefix + "  vs. job wait time till first",
                      "wait-time-first",
                      'Avg. wait time till first sched attempt [sec]',
                      "ms-to-day")

plot_2d_data_set_dict(workload_queue_time_till_fully,
                      string_prefix + "  vs. job wait time till fully",
                      "wait-time-fully",
                      'Avg. wait time till fully scheduled [sec]',
                      "ms-to-day")

plot_2d_data_set_dict(workload_queue_time_till_first_90_ptile,
                      string_prefix + "  vs. 90%tile job wait time till first",
                      "wait-time-first-90ptile",
                      '90%tile wait time till first scheduled [sec]',
                      "ms-to-day")

plot_2d_data_set_dict(workload_queue_time_till_fully_90_ptile,
                      string_prefix + "  vs. 90%tile job wait time till fully",
                      "wait-time-fully-90ptile",
                      '90%tile wait time till fully scheduled [sec]',
                      "ms-to-day")

plot_2d_data_set_dict(workload_num_jobs_unscheduled,
                      string_prefix + "  vs. num jobs unscheduled",
                      "jobs-unscheduled",
                      'Num jobs unscheduled',
                      "abs")

plot_2d_data_set_dict(workload_num_jobs_scheduled,
                      string_prefix + "  vs. num jobs scheduled",
                      "jobs-scheduled",
                      'Num jobs scheduled',
                      "abs")

plot_2d_data_set_dict(workload_num_jobs_timed_out,
                      string_prefix + "  vs. num jobs ignored due to no-fit events",
                      "num-jobs-timed-out",
                      'Num jobs ignored due to failed scheduling',
                      "abs")

plot_2d_data_set_dict(workload_num_jobs_fully_scheduled,
                      string_prefix + "  vs. num jobs fully scheduled",
                      "jobs-fully-scheduled",
                      'Num jobs fully scheduled',
                      "abs")

plot_2d_data_set_dict(workload_avg_job_execution_time,
                      string_prefix + "  vs. avg jobs execution time",
                      "avg-jobs-execution-time",
                      'Avg jobs execution time',
                      "ms-to-day")

plot_2d_data_set_dict(workload_avg_job_completion_time,
                      string_prefix + "  vs. avg job completion time",
                      "avg-jobs-completion-time",
                      'Avg jobs completion time',
                      "ms-to-day")

plot_2d_data_set_dict(workload_avg_ramp_up_time,
                      string_prefix + "  vs. avg job ramp-up time",
                      "avg-jobs-ramp-up-time",
                      'Avg jobs ramp-up time',
                      "ms-to-day")

# SCHEDULER BUSY TIME FRACTION PLOT
plot_2d_data_set_dict(sched_total_busy_fraction,
                      string_prefix + "  vs. busy time fraction",
                      "busy-time-fraction",
                      'Busy time fraction',
                      "0-to-1")

# SCHEDULER CONFLICT FRACTION PLOT
plot_2d_data_set_dict(sched_conflict_fraction,
                      string_prefix + "  vs. conflict fraction",
                      "conflict-fraction",
                      'Conflict fraction',
                      "0-to-1")

# SCHEDULER DAILY BUSY AND CONFLICT FRACTION MEDIANS
plot_2d_data_set_dict(sched_daily_busy_fraction,
                      string_prefix + "  vs. median(daily busy time fraction)",
                      "daily-busy-fraction-med",
                      'Median(daily busy time fraction)',
                      "0-to-1",
                      sched_daily_busy_fraction_err)

plot_2d_data_set_dict(sched_daily_conflict_fraction,
                      string_prefix + "  vs. median(daily conflict fraction)",
                      "daily-conflict-fraction-med",
                      'Median(daily conflict fraction)',
                      "0-to-1",
                      sched_daily_conflict_fraction_err)

# AVERAGE BUSY TIME AND CONFLICT FRACTION ACROSS MULTIPLE SCHEDULERS
plot_2d_data_set_dict(multi_sched_avg_busy_fraction,
                      string_prefix + "  vs. avg busy time fraction",
                      "multi-sched-avg-busy-time-fraction",
                      'Avg busy time fraction',
                      "0-to-1")

plot_2d_data_set_dict(multi_sched_avg_conflict_fraction,
                      string_prefix + "  vs. avg conflict fraction",
                      "multi-sched-avg-conflict-fraction",
                      'Avg conflict fraction',
                      "0-to-1")

# SCHEDULER TASK CONFLICT FRACTION PLOT
plot_2d_data_set_dict(sched_task_conflict_fraction,
                      string_prefix + "  vs. task conflict fraction",
                      "task-conflict-fraction",
                      'Task conflict fraction',
                      "0-to-1")

plot_2d_data_set_dict(sched_num_retried_transactions,
                      string_prefix + "  vs. retried transactions",
                      "retried-transactions",
                      'Num retried transactions',
                      "abs")

plot_2d_data_set_dict(sched_num_jobs_remaining,
                      string_prefix + "  vs. pending jobs at end of sim.",
                      "pending-jobs-at-end",
                      'Num pending jobs at end of sim.',
                      "abs")

plot_2d_data_set_dict(sched_failed_find_victim_attempts,
                      string_prefix + "  vs. failed attempts to find task for machine.",
                      "failed-find-task-for-machine",
                      'Num failed attempts to find task for machine',
                      "abs")

plot_2d_data_set_dict(sched_num_jobs_timed_out,
                      string_prefix + "  vs. num jobs ignored due to no-fit events.",
                      "num-jobs-timed-out-scheduler",
                      'Num jobs ignored due to failed scheduling',
                      "abs")

plot_distribution(workload_job_num_tasks,
                  "Job Tasks distribution",
                  "tasks-distribution",
                  'Num tasks',
                  "abs")

plot_distribution(workload_job_num_moldable_tasks,
                  "Job Moldable Tasks distribution",
                  "tasks-moldable-distribution",
                  'Num tasks',
                  "abs")

plot_distribution(workload_job_mem_tasks,
                  "Job Tasks Memory distribution",
                  "tasks-memory-distribution",
                  'Memory (KB)',
                  "abs")

plot_distribution(workload_job_cpu_tasks,
                  "Job Tasks CPU distribution",
                  "tasks-cpu-distribution",
                  'CPU',
                  "abs")

plot_distribution(workload_job_runtime_tasks,
                  "Job Tasks Runtime distribution",
                  "tasks-runtime-distribution",
                  'Runtime (s)',
                  "abs")
