from os.path import exists
import matplotlib
import matplotlib.pyplot as plt
import matplotlib.font_manager as font_manager
import os

#------------------------------------------------------------------------
# branding (ref: https://news.ucar.edu/sites/default/files/documents/related-links/2020-03/NCAR-UCAR_BrandStandards_031020-Spreads.pdf)
ucar_green='#00797C'
ucar_lighter_green='#28939D'
ucar_lightest_green='40C1AC'
ncar_blue='#1A658F'
ncar_lighter_blue='#007FA3'
ncar_lightest_blue='#00C1D5'
deep_blue='#012169'
hilight_green='#A8C700'

# https://stackoverflow.com/questions/7726852/how-to-use-a-random-otf-or-ttf-font-in-matplotlib
def load_matplotlib_local_fonts():
    # Load a font from TTF file,
    # relative to this Python module
    # https://stackoverflow.com/a/69016300/315168
    font_path = os.path.join(os.path.dirname('__file__'), 'fonts/Poppins-Regular.ttf')
    assert os.path.exists(font_path)
    font_manager.fontManager.addfont(font_path)
    prop = font_manager.FontProperties(fname=font_path)
    matplotlib.rcParams.update({'font.sans-serif': prop.get_name()})

    font_path = os.path.join(os.path.dirname('__file__'), 'fonts/CormorantGaramond-Regular.ttf')
    assert os.path.exists(font_path)
    font_manager.fontManager.addfont(font_path)
    prop = font_manager.FontProperties(fname=font_path)
    matplotlib.rcParams.update({'font.serif': prop.get_name()})

load_matplotlib_local_fonts()

#print('Available Fonts = ', font_manager.get_font_names())
matplotlib.rc('font', family='sans-serif')
#matplotlib.rcParams['font.sans-serif'] = ['Poppins', 'Helvetica', 'Arial', 'DejaVu Sans']
#matplotlib.rcParams['font.serif']      = ['Coromont', 'Garamond', 'Times', 'serif']
#------------------------------------------------------------------------

#if __name__ == "__main__" and "__file__" not in globals():
#    sys.argv[1] = 'derecho.yaml'

#with open(sys.argv[1], 'r') as flh:
#   mach = yaml.safe_load(flh)

def format_ax(ax):
    ax.grid(visible=True, which='major', color='#999999', linestyle='-', zorder=-1)
    ax.minorticks_on()
    ax.grid(visible=True, which='minor', color='#999999', linestyle='-', zorder=-1, alpha=0.2)
    return
