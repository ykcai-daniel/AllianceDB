import getopt
import os
import sys

import matplotlib
import matplotlib.pyplot as plt
import numpy as np
import pylab
from matplotlib.font_manager import FontProperties
from matplotlib.ticker import LinearLocator, LogLocator, MaxNLocator, ScalarFormatter
from numpy import double

OPT_FONT_NAME = 'Helvetica'
TICK_FONT_SIZE = 24
LABEL_FONT_SIZE = 28
LEGEND_FONT_SIZE = 26
LABEL_FP = FontProperties(style='normal', size=LABEL_FONT_SIZE)
LEGEND_FP = FontProperties(style='normal', size=LEGEND_FONT_SIZE)
TICK_FP = FontProperties(style='normal', size=TICK_FONT_SIZE)

MARKERS = (['o', 's', 'v', "^", "h", "v", ">", "x", "d", "<", "|", "", "|", "_"])
# you may want to change the color map for different figures
COLOR_MAP = ('#B03A2E', '#2874A6', '#239B56', '#7D3C98', '#F1C40F', '#F5CBA7', '#82E0AA', '#AEB6BF', '#AA4499')
# you may want to change the patterns for different figures
PATTERNS = (["\\", "///", "o", "||", "\\\\", "\\\\", "//////", "//////", ".", "\\\\\\", "\\\\\\"])
LABEL_WEIGHT = 'bold'
LINE_COLORS = COLOR_MAP
LINE_WIDTH = 5.0
MARKER_SIZE = 0.0
MARKER_FREQUENCY = 1000

matplotlib.rcParams['ps.useafm'] = True
matplotlib.rcParams['pdf.use14corefonts'] = True
matplotlib.rcParams['xtick.labelsize'] = TICK_FONT_SIZE
matplotlib.rcParams['ytick.labelsize'] = TICK_FONT_SIZE
matplotlib.rcParams['font.family'] = OPT_FONT_NAME
matplotlib.rcParams['pdf.fonttype'] = 42

exp_dir = "/data1/xtra"

FIGURE_FOLDER = exp_dir + '/results/figure'

# there are some embedding problems if directly exporting the pdf figure using matplotlib.
# so we generate the eps format first and convert it to pdf.
def ConvertEpsToPdf(dir_filename):
    os.system("epstopdf --outfile " + dir_filename + ".pdf " + dir_filename + ".eps")
    os.system("rm -rf " + dir_filename + ".eps")

class ScalarFormatterForceFormat(ScalarFormatter):
    def _set_format(self):  # Override function that finds format to use.
        self.format = "%1.1f"  # Give format here

# draw a line chart
def DrawFigure(x_values, y_values, sum, legend_labels, x_label, y_label, filename, allow_legend):
    # you may change the figure size on your own.
    fig = plt.figure(figsize=(9, 3))
    figure = fig.add_subplot(111)

    FIGURE_LABEL = legend_labels

    if not os.path.exists(FIGURE_FOLDER):
        os.makedirs(FIGURE_FOLDER)

    # values in the x_xis
    index = np.arange(len(x_values))
    # the bar width.
    # you may need to tune it to get the best figure.
    width = 0.5
    # draw the bars
    bottom_base = np.zeros(len(y_values[0]))
    bars = [None] * (len(FIGURE_LABEL))
    for i in range(len(y_values)):
        bars[i] = plt.bar(index + width / 2, y_values[i], width, hatch=PATTERNS[i], color=LINE_COLORS[i],
                          label=FIGURE_LABEL[i], bottom=bottom_base, edgecolor='black', linewidth=3)
        bottom_base = np.array(y_values[i]) + bottom_base
    x_coordinates = [0, 4]
    y_coordinates = [sum, sum]
    print(sum)

    plt.plot(x_coordinates, y_coordinates,
             color='black', linewidth=LINE_WIDTH,
             marker=MARKERS[4], markersize=MARKER_SIZE,
             markeredgewidth=2, markeredgecolor='k')  # this is the JM line.
    plt.text(3.7, sum+10, "JM", fontproperties=LABEL_FP)
    # sometimes you may not want to draw legends.
    if allow_legend == True:
        plt.legend(bars, FIGURE_LABEL)
        if allow_legend == True:
            handles, labels = figure.get_legend_handles_labels()
        if allow_legend == True:
            leg = plt.legend(handles[::-1], labels[::-1],
                             loc='center',
                             prop=LEGEND_FP,
                             ncol=4,
                             bbox_to_anchor=(0.5, 1.3),
                             handletextpad=0.1,
                             borderaxespad=0.0,
                             handlelength=1.8,
                             labelspacing=0.3,
                             columnspacing=0.4,
                             )
            leg.get_frame().set_linewidth(2)
            leg.get_frame().set_edgecolor("black")

    # you may need to tune the xticks position to get the best figure.
    plt.xticks(index + 0.5 * width, x_values)
    yfmt = ScalarFormatterForceFormat()
    yfmt.set_powerlimits((0,0))
    figure.get_yaxis().set_major_formatter(yfmt)
    plt.ticklabel_format(axis="y", style="sci", scilimits=(0,0), useMathText=True)
    plt.grid(axis='y', color='gray')
    figure.yaxis.set_major_locator(LinearLocator(3))
    # figure.yaxis.set_major_locator(LinearLocator(6))
    # figure.yaxis.set_major_locator(LogLocator(base=10))
    figure.get_xaxis().set_tick_params(direction='in', pad=10)
    figure.get_yaxis().set_tick_params(direction='in', pad=10)

    plt.xlabel(x_label, fontproperties=LABEL_FP)
    plt.ylabel(y_label, fontproperties=LABEL_FP)

    plt.savefig(FIGURE_FOLDER + "/" + filename + ".pdf", bbox_inches='tight', format='pdf')


def DrawLegend(legend_labels, filename):
    fig = pylab.figure()
    ax1 = fig.add_subplot(111)
    FIGURE_LABEL = legend_labels
    LEGEND_FP = FontProperties(style='normal', size=26)

    bars = [None] * (len(FIGURE_LABEL))
    data = [1]
    x_values = [1]

    width = 0.3
    for i in range(len(FIGURE_LABEL)):
        bars[i] = ax1.bar(x_values, data, width, hatch=PATTERNS[i], color=LINE_COLORS[i],
                          linewidth=0.2)

    # LEGEND
    figlegend = pylab.figure(figsize=(11, 0.5))
    figlegend.legend(bars, FIGURE_LABEL, prop=LEGEND_FP, \
                     loc=9,
                     bbox_to_anchor=(0, 0.4, 1, 1),
                     ncol=len(FIGURE_LABEL), mode="expand", shadow=False, \
                     frameon=False, handlelength=1.1, handletextpad=0.2, columnspacing=0.1)

    figlegend.savefig(FIGURE_FOLDER + '/' + filename + '.pdf')


def normalize(y_values):
    y_total_values = np.zeros(len(y_values[0]))

    for i in range(len(y_values)):
        y_total_values += np.array(y_values[i])
    y_norm_values = []

    for i in range(len(y_values)):
        y_norm_values.append(np.array(y_values[i]) / (y_total_values * 1.0))
    return y_norm_values


# example for reading csv file
def ReadFile(id):
    # Creates a list containing w lists, each of h items, all set to 0
    w, h = 4, 4
    y = [[0 for x in range(w)] for y in range(h)]
    # print(matches)
    max_value = 0
    j = 0
    bound = id + 1 * w
    for i in range(id, bound, 1):
        cnt = 0
        print(i)
        f = open(exp_dir + "/results/breakdown/PMJ_JBCR_NP_{}.txt".format(i), "r")
        read = f.readlines()
        others = 0
        for x in read:
            value = double(x.strip("\n"))
            if cnt == 1:  # partition
                y[0][j] = value
            elif cnt == 3:  # sort
                y[1][j] = value
            elif cnt == 4:  # merge
                y[2][j] = value
            elif cnt == 5:  # join
                y[3][j] = value
            else:
                others += value
            # if cnt == 6:
            #     y[2][j] = others
            cnt += 1
        j += 1
    f = open(exp_dir + "/results/breakdown/PMJ_JM_NP_{}.txt".format(id), "r")
    read = f.readlines()
    sum = 0
    cnt = 0
    for x in read:
        value = double(x.strip("\n"))
        if cnt == 1:  # partition
            sum += value
        elif cnt == 3:  # sort
            sum += value
        elif cnt == 4:  # merge
            sum += value
        elif cnt == 5:  # join
            sum += value
        cnt += 1
    print(y)
    print(sum)
    return y, sum, max_value


if __name__ == "__main__":
    id = 124
    try:
        opts, args = getopt.getopt(sys.argv[1:], '-i:h', ['test id', 'help'])
    except getopt.GetoptError:
        print('breakdown.py -id testid')
        sys.exit(2)
    for opt, opt_value in opts:
        if opt in ('-h', '--help'):
            print("[*] Help info")
            exit()
        elif opt == '-i':
            print('Test ID:', opt_value)
            id = (int)(opt_value)

    x_values = [1, 2, 4, 8]  # merging step size

    y_values, sum, max_value = ReadFile(id)  #

    # y_norm_values = normalize(y_values)

    # break into 4 parts
    legend_labels = ['partition', 'sort', 'merge', 'probe']  # , 'others'

    DrawFigure(x_values, y_values, sum, legend_labels,
               'group size', 'cycles per input tuple',
               'breakdown_group_pmj_figure', True)

    # DrawLegend(legend_labels, 'breakdown_radix_legend')
