
import sys
import argparse
import json
import time

def read_metric(filename):

    f = open(filename, 'r')

    content = []
    begin_time_in_secs = 0
    for line in f:
        metric = json.loads(line)
        if "status" in metric and metric["status"] == "success":
            if "end" in metric and "start" in metric:
                s_time_in_secs = metric["start"]["$date"] / 1000.0
                e_time_in_secs = metric["end"]["$date"] / 1000.0
                start_time = time.gmtime(s_time_in_secs)
                end_time = time.gmtime(e_time_in_secs)
                duration = e_time_in_secs - s_time_in_secs

                if begin_time_in_secs == 0:
                    begin_time_in_secs = s_time_in_secs
                else:
                    begin_time_in_secs = min(begin_time_in_secs, s_time_in_secs)

                record = {"start": s_time_in_secs, "end": e_time_in_secs, "duration": duration}

                content.append(record)
    f.close()

    content = sorted(content, key = lambda metric: metric["start"])

    return content

def write_file(data, filename):

    f = open(filename, 'w')

    for item in data:
        f.write(item + "\n")

    f.close()

def main():
    parser = argparse.ArgumentParser(description = 'Parse Spark cluster usage metric.')
    parser.add_argument('-f', help = 'The file containing usage metric.')
    parser.add_argument('-o', help = 'The output filename prefix.')

    args = parser.parse_args()
    metrics = read_metric(args.f)

    predata = "example_cluster cmb-new "

    interarrival_time_output = []
    duration_output = []

    prev_metric = None
    for metric in metrics:
        interarrival_time = 0
        if prev_metric != None:
            interarrival_time = metric["start"] - prev_metric["start"]

        prev_metric = metric

        duration = metric["duration"]

        # interarrival_time
        interarrival_time_output.append(predata + "0 " + str(interarrival_time))
        interarrival_time_output.append(predata + "1 " + str(interarrival_time))
 
        # duration
        duration_output.append(predata + "0 " + str(duration))
        duration_output.append(predata + "1 " + str(duration))

    write_file(interarrival_time_output, args.o + "_interarrival_time.log")
    write_file(duration_output, args.o + "_duration.log")

if __name__ == "__main__":
    main()

