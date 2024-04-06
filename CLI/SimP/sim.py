import os
import pandas as pd
import dask
dask.config.set({'dataframe.query-planning': True})


import dask.dataframe as dd
import holoviews as hv
from holoviews import dim, opts
import plotly.express as px
import argparse
import numpy as np

# Ensure you have matplotlib and bokeh backends enabled
hv.extension('matplotlib')
hv.extension('bokeh')


def read_combine_parquets(input_dir):
    """
    Reads and combines parquet files from a given directory.
    Adds a 'filename' column to distinguish between different files.
    """
    dfs = []
    for file in sorted(os.listdir(input_dir)):
        if file.endswith('.parquet'):
            file_path = os.path.join(input_dir, file)
            df = dd.read_parquet(file_path).compute()
            filename = os.path.basename(file_path).split('.')[0]
            df['filename'] = filename
            dfs.append(df)

    combined_df = pd.concat(dfs)
    
    # Define the desired order of serotypes
    serotype_order = ['1', '2', '3', '4']  # Extend this list as needed
    
    # Convert 'serotype' to a categorical type with the specified order
    combined_df['serotype'] = pd.Categorical(combined_df['serotype'], categories=serotype_order, ordered=True)

    return combined_df

def filter_dataframe(df, column='serotype', filter_value='Unassigned'):
    """
    Filters the DataFrame based on a specified column and filter value.
    Includes diagnostic print statements to track the filtering impact.
    """
    # Diagnostic: Count of rows per filename before filtering
    print("Rows per filename before filtering:")
    print(df.groupby('filename').size())
    
    filtered_df = df[df[column] != filter_value]
    
    # Diagnostic: Count of rows per filename after filtering
    print("Rows per filename after filtering:")
    print(filtered_df.groupby('filename').size())
    
    return filtered_df

def generate_gc_box_plot(df):
    """
    Generates a GC percentage box plot grouped by filename.
    """
    gc_box_plots = hv.BoxWhisker(df, ['filename'], 'gc_percentage', label='GC Percentage by Filename').opts(tools=['hover', 'save', 'pan', 'box_zoom', 'wheel_zoom', 'reset'], width=1000, height=600,xrotation=90  )
    return gc_box_plots


def generate_stacked_bars(df):
    """
    Generates stacked bar plots for serotype frequencies by filename.
    """
    pivot_df = df.pivot_table(index='filename', columns='serotype', aggfunc='size', fill_value=0)
    long_df = pivot_df.stack().reset_index().rename(columns={0: 'Frequency'})
    stacked_bars = hv.Bars(long_df, ['filename', 'serotype'], 'Frequency').opts(
        stacked=True,
        title='Serotype Frequencies for each Serotype by Filenames',
        xlabel='Filename',
        ylabel='Frequency',
        width=1200,
        height=600,
        tools=['hover', 'save', 'pan', 'box_zoom', 'wheel_zoom', 'reset'],  # Added more interactive tools
        legend_position='right',
        color=hv.Cycle('Category20'),
        xrotation=90  
    )
    return stacked_bars


# Assuming other necessary imports and functions are already in your script

def generate_total_coverage_heatmap(df):
    """
    Generates a heatmap for total coverage by filename and serotype, with aggregate overlay.
    """
    # Pivot to get total coverage by filename and serotype
    coverage_pivot = df.pivot_table(values='total_coverage', index='filename', columns='serotype', aggfunc='mean').fillna(0)
    coverage_pivot_reset = coverage_pivot.reset_index()
    coverage_long = coverage_pivot_reset.melt(id_vars=['filename'], var_name='serotype', value_name='total_coverage')
    
    # Create HeatMap
    heatmap = hv.HeatMap(coverage_long, ['filename', 'serotype'], 'total_coverage', label='Total Coverage Heatmap')
    
    # Aggregate data for overlay
    aggregate = hv.Dataset(heatmap).aggregate('filename', function=np.mean, spreadfn=np.std)
    
    # Example marker (adjust as needed)
    vline = hv.VLine(x=10)  # Example: x=10, adjust this based on your data
    marker = hv.Text(x=11, y=800, text='Significant Event', halign='left')  # Adjust position and text as needed
    
    agg = hv.ErrorBars(aggregate) * hv.Curve(aggregate)
    
    # Combine plots
    coverage_heatmap = (heatmap + agg * vline * marker).cols(1)
    coverage_heatmap.opts(
        opts.HeatMap(width=900, height=500, tools=['hover'], logz=True, 
                    invert_yaxis=True, labelled=[], toolbar='above',
                    xaxis=None, colorbar=True, clim=(1, np.nan)),
        opts.VLine(line_color='black'),
        opts.ErrorBars(line_width=2, line_color='red'),
        opts.Curve(tools=['hover'], line_width=2),
        opts.Overlay(width=900, height=400, show_title=False, xrotation=90)
    )
    
    return coverage_heatmap

def generate_serotype_frequency_heatmap(df_filtered):
    """
    Generates a heatmap of serotype frequency by filename.
    """
    heatmap_df = df_filtered.pivot_table(index='serotype', columns='filename', aggfunc='size', fill_value=0)
    long_df = heatmap_df.stack().reset_index().rename(columns={0: 'Frequency'})
    heatmap = hv.HeatMap(long_df, ['filename', 'serotype'], 'Frequency').opts(
        tools=['hover', 'save', 'pan', 'box_zoom', 'wheel_zoom', 'reset'],  # Added more interactive tools, 
        width=1000, 
        height=600, 
        colorbar=True, 
        cmap='YlGnBu', 
        xlabel='Filename', 
        ylabel='Serotype', 
        title='Heatmap of Serotype Frequency by Filename',
        xrotation=90,

    )
    return heatmap


def generate_b_score_heatmap(df_filtered):
    """
    Generates a heatmap for maximum B score across all files, with filenames on the x-axis
    and serotypes on the y-axis.
    """
    # Aggregate the maximum B score for each combination of filename and serotype
    agg_df = df_filtered.groupby(['filename', 'serotype'])['b_score'].max().reset_index()
    
    # Create a HeatMap
    heatmap = hv.HeatMap(agg_df, ['filename', 'serotype'], 'b_score').opts(
        tools=['hover', 'save', 'pan', 'box_zoom', 'wheel_zoom', 'reset'],
        width=1000, height=600, xlabel='Filename', ylabel='Serotype',
        title='Maximum B Score Heatmap',
        colorbar=True,
        cmap='Viridis',  # You can choose a different colormap if you prefer
        xrotation=90  # Rotate the x-axis labels for better readability
    )
    
    return heatmap



def generate_b_score_curve_plot(df_filtered):
    """
    Generates curve plots for average B score by serotype.
    """
    # Ensure b_score is a numeric type for aggregation
    df_filtered['b_score'] = pd.to_numeric(df_filtered['b_score'], errors='coerce')
    
    # Group by serotype and calculate the mean b_score
    grouped = df_filtered.groupby('serotype')['b_score'].mean().reset_index()
    
    # Sort serotypes if they are not numeric
    grouped['serotype'] = pd.Categorical(grouped['serotype'])
    grouped = grouped.sort_values('serotype')
    
    # Generate a Curve plot
    curve = hv.Curve(grouped, 'serotype', 'b_score').opts(
        tools=['hover', 'save', 'pan', 'box_zoom', 'wheel_zoom', 'reset'],
        width=800, height=400, xlabel='Serotype', ylabel='Average B Score',
        title='Average B Score by Serotype'
    )
    
    return curve




def generate_b_score_box_plots(df_filtered):
    """
    Generates box plots for the distribution of b_score by serotype.
    """
    # Ensure b_score is numeric, coercing errors to NaN
    df_filtered['b_score'] = pd.to_numeric(df_filtered['b_score'], errors='coerce')
    
    # Ensure 'serotype' is treated as a categorical variable with the desired order
    serotype_order = ['1', '2', '3', '4']  # Extend this list as needed
    df_filtered['serotype'] = pd.Categorical(df_filtered['serotype'], categories=serotype_order, ordered=True)
    
    # Generate a BoxWhisker plot for b_score by serotype
    box_plot = hv.BoxWhisker(df_filtered, 'serotype', 'b_score').opts(
        opts.BoxWhisker(height=400, width=600, xlabel='Serotype', ylabel='B Score',
                        tools=['hover'], box_fill_color=hv.Cycle('Category20'))
    )
    
    return box_plot


def generate_violin_plots(df_filtered):
    """
    Generates violin plots for each numerical column except those specified in exclude_columns.
    """
    exclude_columns = ['read_id', 'matched_sanket', 'serotype', 'gc_percentage']
    plots = []
    
    # Ensure 'serotype' is treated as a categorical variable with the desired order
    serotype_order = ['1', '2', '3', '4']  # Extend this list as needed
    df_filtered['serotype'] = pd.Categorical(df_filtered['serotype'], categories=serotype_order, ordered=True)
    
    for column in df_filtered.columns:
        if column not in exclude_columns and pd.api.types.is_numeric_dtype(df_filtered[column]):
            # Generate violin plot for each numerical column, ensuring 'serotype' is ordered
            violin = hv.Violin(df_filtered, 'serotype', column).opts(opts.Violin(height=400, width=600))
            plots.append(violin)
    layout = hv.Layout(plots).cols(3)
    return layout

def generate_heatmap(df_filtered, count_column, title):
    """
    Generates a heatmap for the specified count column ('p_count' or 'ssr_count') 
    aggregated by 'serotype' and 'filename'.
    """
    # Ensure count is numeric, coercing errors to NaN
    df_filtered.loc[:, count_column] = pd.to_numeric(df_filtered[count_column], errors='coerce')

    # Aggregate data by serotype and filename for total counts
    agg_df = df_filtered.groupby(['filename','serotype']).agg({count_column: 'sum'}).reset_index()
    
    # Create a HeatMap
    h2 = hv.HeatMap(agg_df, [ 'filename','serotype'], count_column).opts(
        tools=['hover', 'save', 'pan', 'box_zoom', 'wheel_zoom', 'reset'],  # Added more interactive tools

        width=1000, 
        height=600, 
        colorbar=True, 
        cmap='Magma', 
        xlabel='Filename', 
        ylabel='Serotype', 
        title=title,
        xrotation=90
        
    )
    return h2



# Parse command-line arguments for input and output directories
parser = argparse.ArgumentParser(description='Process some integers.')
parser.add_argument('-i', '--input_dir', required=True, help='Input directory containing parquet files')
parser.add_argument('-o', '--output_dir', required=True, help='Output directory for saving plots')

args = parser.parse_args()

input_dir = args.input_dir
output_dir = args.output_dir


# Create the output directory if it doesn't exist
if not os.path.exists(output_dir):
    os.makedirs(output_dir)

df_combined = read_combine_parquets(input_dir)
df_filtered = filter_dataframe(df_combined)

# Generate and save HoloViews plots as HTML
gc_box_plot = generate_gc_box_plot(df_filtered)
hv.save(gc_box_plot, os.path.join(output_dir, 'gc_box_plot.html'), fmt='html')

stacked_bar_plot = generate_stacked_bars(df_filtered)
hv.save(stacked_bar_plot, os.path.join(output_dir, 'stacked_bar_plot.html'), fmt='html')

total_coverage_heatmap = generate_total_coverage_heatmap(df_filtered)
hv.save(total_coverage_heatmap, os.path.join(output_dir, 'total_coverage_heatmap.html'), fmt='html')


b_score_box_plots = generate_b_score_box_plots(df_filtered)
hv.save(b_score_box_plots, os.path.join(output_dir, 'b_score_box_plots.html'), fmt='html')

b_score_curve_plot = generate_b_score_curve_plot(df_filtered)
hv.save(b_score_curve_plot, os.path.join(output_dir, 'b_score_curve_plot.html'), fmt='html')

b_score_heatmap = generate_b_score_heatmap(df_filtered)
hv.save(b_score_heatmap, os.path.join(output_dir, 'b_score_heatmap.html'), fmt='html')

violin_plots = generate_violin_plots(df_filtered)
hv.save(violin_plots, os.path.join(output_dir, 'violin_plots.html'), fmt='html')

serotype_frequency_heatmap = generate_serotype_frequency_heatmap(df_filtered)
hv.save(serotype_frequency_heatmap, os.path.join(output_dir, 'serotype_frequency_heatmap.html'), fmt='html')

p_count_heatmap = generate_heatmap(df_filtered, 'p_count', 'P Count Heatmap by Serotype and Filename')
hv.save(p_count_heatmap, os.path.join(output_dir, 'p_count_heatmap.html'), fmt='html')

ssr_count_heatmap = generate_heatmap(df_filtered, 'ssr_count', 'SSR Count Heatmap by Serotype and Filename')
hv.save(ssr_count_heatmap, os.path.join(output_dir, 'ssr_count_heatmap.html'), fmt='html')

# Save Plotly treemap as HTML
agg_df = df_filtered.groupby(['filename', 'serotype'])['b_score'].sum().reset_index()
fig = px.treemap(agg_df, path=['filename', 'serotype'], values='b_score',
                title='Sum of B Score for Each Serotype by Filename',
                color='b_score', color_continuous_scale='RdBu')
fig.write_html(os.path.join(output_dir, 'treemap_b_score.html'))  # Save treemap as HTML


