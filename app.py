import sys
print(sys.executable)
import plotly.express as px
import pandas as pd
import dash_bootstrap_components as dbc # Dash Bootstrap components
from dash import (
    Dash, 
    html, 
    dcc,
    exceptions
)
from dash.dependencies import (
    Input, 
    Output, 
    State
)
from dash.exceptions import PreventUpdate
from ui_data import (
    MongoData
)

app = Dash(__name__, external_stylesheets=[dbc.themes.SANDSTONE],
        meta_tags=[{'name': 'viewport', 'content': 'width=device-width, initial-scale=1'},], suppress_callback_exceptions=True)
        
# the style arguments for the sidebar. We use position:fixed and a fixed width
SIDEBAR_STYLE = {
    "position": "fixed",
    "top": 0,
    "left": 0,
    "bottom": 0,
    "width": "16rem",
    "padding": "2rem 1rem",
    "background-color": "#f8f9fa",
}

# the styles for the main content position it to the right of the sidebar and
# add some padding.
CONTENT_STYLE = {
    "margin-left": "18rem",
    "margin-right": "2rem",
    "padding": "2rem 1rem",
}

sidebar = html.Div(
    [
        html.H2("LyoHub", className="display-5"),
        html.H2("Drugs Dashboard", className="display-5"),
        html.Hr(),
        dbc.Nav(
            [dbc.NavLink("Home", href="/", active="exact")],
            vertical=True,
            pills=True,
        ),
        html.Hr(),
        html.H2("Dashboards", className="lead"),
        dbc.Nav(
            [
                dbc.NavLink("Display Tables", href="/tables", active="exact"),
                dbc.NavLink("Display Charts", href="/charts", active="exact"),
                dbc.NavLink("Display Time-Series Data", href="/time_series", active="exact"),
            ],
            vertical=True,
            pills=True,
        ),
    ],
    style=SIDEBAR_STYLE,
)

content = html.Div(id="page-content", style=CONTENT_STYLE)

# Sidebar layout
app.layout = html.Div([dcc.Location(id="url"), sidebar, content])

UI_DATA_OBJ = MongoData()
ACTIVE_CHART, INACTIVE_CHART, ACTIVE_TIMESERIES, INACTIVE_TIMESERIES = UI_DATA_OBJ.get_timeseries_dataframe()
PRODUCTS_LIST, ACTIVE_INGREDIENTS_LIST, INACTIVE_INGREDIENTS_LIST = UI_DATA_OBJ.get_search_bar_data()

'''
*********************
----- HOME PAGE -----
*********************
'''
@app.callback(Output("page-content", "children"), [Input("url", "pathname")])
def render_page_content(pathname):
    if pathname == '/':
        return html.Div([dcc.Markdown('''
            ### Application Overview
            This application is developed at LyoHub to help the users search through various Lyophilized drugs approved by FDA.
            There are two sections on the left panel to navigate through different visualtizations provided by the application.
            The Display Tables tab display different drugs along with their active and ingredients in a table format, while the
            Display Charts tab display various active and inactive ingredients along with the number of drugs they are present in
            a bar chart format.
        ''')],className='home')
    
    elif pathname == '/tables':
        return generate_table_page()
    elif pathname == '/charts':
        return generate_chart()
    elif pathname == '/time_series':
        return generate_time_series()



'''
***********************
----- CHARTS PAGE -----
***********************
'''



def generate_chart():
    return html.Div(
    id="root",
    children=[
        html.Div([dcc.Markdown('''
            ### Ingredients for Lyophilized Drugs
            The data projected here is obtained from [DailyMed](https://dailymed.nlm.nih.gov/dailymed/) website.
            These are the various active and inactive ingredients cummulative to the different Lyophilized drugs
            found on [DailyMed](https://dailymed.nlm.nih.gov/dailymed/).
            ''')],className='home'
        ),
        html.Div(
            id="app-container",
            children=[
                html.Div(
                    id="graph-container",
                    children=[
                        html.P(id="chart-selector", children="Select chart:"),
                        dcc.Dropdown(
                            options=[
                                {
                                    "label": "Active Ingredients",
                                    "value": "show_active_ingredients",
                                },
                                {
                                    "label": "Inactive Ingredients",
                                    "value": "show_inactive_ingredients",
                                },
                            ],
                            value="show_active_ingredients",
                            id="chart-dropdown",
                        ),
                        dcc.Graph(
                            id="selected-data",
                            figure=dict(
                                data=[dict(x=0, y=0)],
                                layout=dict(
                                    paper_bgcolor="#F4F4F8",
                                    plot_bgcolor="#F4F4F8",
                                    autofill=True,
                                    margin=dict(t=75, r=50, b=50, l=50),
                                ),
                            ),
                        ),
                    ],
                ),
            ],
        ),
    ],
)

@app.callback(
    Output("selected-data", "figure"),
    Input("chart-dropdown", "value")
)
def display_selected_data(chart_dropdown):
    if chart_dropdown == "show_active_ingredients":
        
        fig = px.bar(ACTIVE_CHART, x='Ingredient', y='Occurences',
             hover_data=['Ingredient'], color='Occurences',
             labels={'Ingredient':'Active Ingredient'}, height=1000)
        return fig

    if chart_dropdown == "show_inactive_ingredients":
        
        fig = px.bar(INACTIVE_CHART, x='Ingredient', y='Occurences',
             hover_data=['Ingredient'], color='Occurences',
             labels={'Ingredient':'Inactive Ingredient'}, height=1000)
        return fig


'''
***********************
----- TABLES PAGE -----
***********************
'''

def generate_table_page():
    return html.Div([
                html.Div([
                    dcc.Dropdown(
                        id='table_dropdown',
                        options=[
                            {
                                "label": "Products",
                                "value": "product_dropdown",
                            },
                            {
                                "label": "Active Ingredients",
                                "value": "active_dropdown",
                            },
                            {
                                "label": "Inactive Ingredients",
                                "value": "inactive_dropdown",
                            },
                        ],
                        placeholder="Select type to perform search..",
                        style={'color': 'black', 'backgroundColor': 'white', 'width': '100%'}
                    ),
                    dcc.Dropdown(
                        id='selection_dropdown',
                        placeholder="Select a specific value..",
                        style={'color': 'black', 'backgroundColor': 'white', 'width': '102%'}
                    )
                ], style=dict(display='flex')),
                html.Div(
                    id='output_container',
                    className="container",
                    style={'width': '105%', 'padding-top': '50px'}                                 
                )
            ])

@app.callback(
    Output("selection_dropdown", "options"),
    Input("table_dropdown", "value")
)
def get_selection_options(table_option):
    if table_option == "product_dropdown":
        return [{'label': i, 'value': i} for i in sorted(PRODUCTS_LIST)]
    elif table_option == "active_dropdown":
        return [{'label': i, 'value': i} for i in sorted(ACTIVE_INGREDIENTS_LIST)]
    elif table_option == "inactive_dropdown":
        return [{'label': i, 'value': i} for i in sorted(INACTIVE_INGREDIENTS_LIST)]
    else:
        raise PreventUpdate

def get_custom_tag(items_list):

    if len(items_list) > 0 and items_list[-1] == "products":
        app_number = items_list[0][-1]
        items_len = len(items_list[0])
        return html.Td([html.Strong([items_list[0][i] for i in range(0, items_len-1)], className="book-title"), html.Span([app_number], className="text-offset")], className="item-stock", rowSpan=f"{items_list[1]}")
    else:
        return html.Td([date for date in items_list[0]], className="item-stock", rowSpan=f"{items_list[1]}")

@app.callback(
    Output("output_container", "children"),
    [ 
        Input("selection_dropdown", "value"),
        Input("table_dropdown", "value")
        #Input("inactive_dropdown", "value")
    ]
)
def display_table(selection_dropdown, table_dropdown):

    product_dropdown = None
    active_dropdown = None
    inactive_dropdown = None
    if table_dropdown == "product_dropdown":
        product_dropdown = selection_dropdown
    elif table_dropdown == "active_dropdown":
        active_dropdown = selection_dropdown
    elif table_dropdown == "inactive_dropdown":
        inactive_dropdown = selection_dropdown
        
    records_rows = UI_DATA_OBJ.get_table_data(product_search=product_dropdown, 
                                              active_search=active_dropdown,
                                              inactive_search=inactive_dropdown)

    return html.Table([
                html.Thead([
                    html.Tr([
                        html.Th([
                            "Date"
                        ], scope="col", style={'width': '10%'}),
                        html.Th([
                            "Products"
                        ], scope="col"),
                        html.Th([
                            "Company"
                        ], scope="col"),
                        html.Th([
                            "DailyMed Label"
                        ], scope="col"),
                        html.Th([
                            "Active Ingredient"
                        ], scope="col"),
                        html.Th([
                            "Strength"
                        ], scope="col"),
                        html.Th([
                            "Inactive Ingredient"
                        ], scope="col"),
                        html.Th([
                            "Strength"
                        ], scope="col")
                    ])
                ]),
                html.Tbody([html.Tr([get_custom_tag(value) if isinstance(value[0], list) else html.Td([value[0]], className="item-stock", rowSpan=f"{value[1]}") for value in row]) for row in records_rows]) 
            ])


'''
*************************************************
----- INACTIVE INGREDIENTS TIME SERIES PAGE -----
*************************************************
'''


def get_era_marks():
    era_marks = {}
    for i in range(1954, 2023, 4):
        era_marks.update({i: {'label': str(i)}})
    
    return era_marks

menuSlider = html.Div([
    dbc.Row(dbc.Col(dcc.RangeSlider(
        id='era-slider',
        min=1954,
        max=2022,
        step=1,
        marks=get_era_marks(),
        tooltip={'always_visible': False, 'placement': 'bottom'}))),
    dbc.Row(dbc.Col(html.P(style={'font-size': '16px', 'opacity': '70%'},children='Adjust slider to desired range.'))),
],className='era-slider', style={'padding-top': '50px'})


def generate_time_series():
    return  html.Div([
                    dcc.Dropdown(
                        id='inactive_time_series_dropdown',
                        options=[{'label': i, 'value': i} for i in sorted(INACTIVE_INGREDIENTS_LIST)],
                        placeholder="Choose an Inactive Ingredient",
                        style={'color': 'black', 'backgroundColor': 'white', 'width': '100%'},
                        multi=True,
                        #value=["sucrose"]),
                    ),
                    menuSlider,
                    dcc.Graph(
                        id="time_series_chart",
                        figure=dict(
                            data=[dict(x=0, y=0)],
                            layout=dict(
                                paper_bgcolor="#F4F4F8",
                                plot_bgcolor="#F4F4F8",
                                autofill=True,
                                margin=dict(t=75, r=50, b=50, l=50),
                            ),
                        ),
                    ),
                    html.Div(
                        id='timeseries_table',
                        className="container",
                        style={'width': '106%', 'padding-top': '50px'}                              
                    )
                ])


@app.callback(
    Output("timeseries_table", "children"),
    [ 
        Input("inactive_time_series_dropdown", "value")
    ]

)
def display_timeseries_table(inactive_time_series_dropdown):

    if not inactive_time_series_dropdown:
        raise PreventUpdate

    records_rows = UI_DATA_OBJ.get_table_data(inactive_search=inactive_time_series_dropdown)

    return html.Table([
                html.Thead([
                    html.Tr([
                        html.Th([
                            "Date"
                        ], scope="col", style={'width': '10%'}),
                        html.Th([
                            "Products"
                        ], scope="col"),
                        html.Th([
                            "Company"
                        ], scope="col"),
                        html.Th([
                            "DailyMed Label"
                        ], scope="col"),
                        html.Th([
                            "Active Ingredient"
                        ], scope="col"),
                        html.Th([
                            "Strength"
                        ], scope="col"),
                        html.Th([
                            "Inactive Ingredient"
                        ], scope="col"),
                        html.Th([
                            "Strength"
                        ], scope="col")
                    ])
                ]),
                html.Tbody([html.Tr([get_custom_tag(value) if isinstance(value[0], list) else html.Td([value[0]], className="item-stock", rowSpan=f"{value[1]}") for value in row]) for row in records_rows]) 
            ])

@app.callback(
    Output("time_series_chart", "figure"),
    [Input("inactive_time_series_dropdown", "value"),
    Input("era-slider", "value")]
)
def display_timeseries_graph(inactive_time_series_dropdown, year_range):

    if not inactive_time_series_dropdown:
        raise PreventUpdate

    inactive_df = None
    if inactive_time_series_dropdown:
        inactive_df = INACTIVE_TIMESERIES[INACTIVE_TIMESERIES['Ingredient'].isin(inactive_time_series_dropdown)]
        
        if year_range:
            inactive_df = inactive_df[(inactive_df.Year >= year_range[0] )&( inactive_df.Year <= year_range[1] )]
        else:
            inactive_df = inactive_df[( inactive_df.Year >= 1954 )&( inactive_df.Year <= 2022)]
     
    fig = px.line(inactive_df, x='Year', y='Occurences', color='Ingredient')
    fig.update_traces(mode='markers+lines')
    return fig


        
# Call app server
if __name__ == '__main__':
    # set debug to false when deploying app
    app.run_server(debug=False, port=8050)
