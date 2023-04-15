import dash
from dash import dcc
from dash import html
import dash_bootstrap_components as dbc
import pandas as pd
import plotly.express as px
import plotly.io as pio
import plotly.graph_objects as go
import skimage.io as skio
pio.templates.default = "seaborn"
COVID_IMG = "https://cdn.pixabay.com/photo/2020/04/29/07/54/coronavirus-5107715_960_720.png"
COVID_TABLE = "https://i.imgur.com/1ImiFxr.png"
COVID_STATE = "https://i.imgur.com/c67XX8x.png"

df1 = pd.read_csv('./notebooks/df1.csv')
df2 = pd.read_csv('./notebooks/df2.csv')
df1.date = pd.to_datetime(df1.date)
df1.sort_values(by='date', inplace = True)
df1['date'] = df1['date'].dt.strftime('%m/%d/%Y')
app = dash.Dash(__name__, external_stylesheets=[dbc.themes.ZEPHYR])

############################## Spreading Map Plot ################################

fig_map = px.density_mapbox(df1, lat='latitude', lon='longtitude', z='confirmed', radius=30,zoom=1.5, hover_data={"latitude":False, "longtitude":False, "deaths":':.0f',"recovered":':.0f',"confirmed":':.0f'},
                        mapbox_style="open-street-map", animation_frame = 'date', animation_group= "country_region", range_color= [0, 10000])
fig_map.update_layout(margin={"r":0,"t":0,"l":0,"b":0}, height=800, showlegend= False)    
fig_map.layout.updatemenus[0].buttons[0].args[1]["frame"]["duration"] = 125 # buttons
fig_map.layout.updatemenus[0].buttons[0].args[1]["transition"]["duration"] = 150

############################## navigation bar ################################

navbar = dbc.Navbar( id = 'navbar', children = [
    html.A(
    dbc.Row([
        dbc.Col(),
        dbc.Col(html.Img(src = COVID_IMG, height = "70px")),
        dbc.Col(dbc.NavbarBrand(" Covid-19 Live Tracker - Spreading", style = {'color':'black', 'fontSize':'25px'}))
        ],align = "center",)),dbc.Row([
        dbc.Col(dbc.Button(id = 'button', children = "Reload Data", color = "primary", className = 'ml-auto', href = '/'))],
        className="g-0 ms-auto flex-nowrap mt-3 mt-md-0")
    ]
)
def create_image_figure(filename):
   ron = skio.imread(filename)
   figure = px.imshow(ron)
   figure.update_layout(
         xaxis={
            'showgrid': False, 
            'zeroline': False, 
            'visible': False,  
         },
         yaxis={
            'showgrid': False,
            'zeroline': False,
            'visible': False,
         },
         margin={"r":0,"t":0,"l":0,"b":0}
       )
   return figure
############################## Layout ################################
app.layout =    html.Div([
                            navbar,
                            dcc.Graph 
                                (
                                    id='timeseries',
                                    config={'displayModeBar': False}, 
                                    animate=True,
                                    figure=go.Figure(fig_map)
                                ),
                                                            html.Div([
                            dbc.Row([dbc.Col(dcc.Graph(id='1-img',config={'displayModeBar': False},figure=create_image_figure("./notebooks/1.png")),width=5),
                                    dbc.Col(dcc.Graph(id='2-img',config={'displayModeBar': False},figure=create_image_figure("./notebooks/2.png")), width=3, style={'display':'inline-block','vertical-align':'top'}),
                                    ],
                                    justify='center', align='start'),
                            ]),
                        ])

# Run the app
if __name__ == '__main__':
    app.run_server(debug=True)
