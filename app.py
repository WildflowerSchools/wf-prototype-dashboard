import honeycomb_io
import dash
import dash_core_components as dcc
import dash_html_components as html
from dash.dependencies import Input, Output
import dash_table
from flask_caching import Cache
import pandas as pd
import datetime
import dateutil
import uuid
import logging

logging.basicConfig(level=logging.INFO)

logger = logging.getLogger(__name__)

app = dash.Dash(__name__)

cache = Cache(app.server, config={
    'CACHE_TYPE': 'redis',
    # Note that filesystem cache doesn't work on systems with ephemeral
    # filesystems like Heroku.
    'CACHE_TYPE': 'filesystem',
    'CACHE_DIR': 'cache-directory',

    # should be equal to maximum number of users on the app at a single time
    # higher numbers will store more data in the filesystem / redis cache
    'CACHE_THRESHOLD': 200
})

def serve_layout():
    session_id = str(uuid.uuid4())
    return html.Div([
        html.Div(session_id, id='session-id', style={'display': 'none'}),
        html.Div([
            html.Div(
                [
                    html.H4('Date range'),
                    dcc.DatePickerRange(
                        id='my-date-picker-range',
                        min_date_allowed=datetime.date(2020, 8, 1),
                        max_date_allowed=datetime.date(2021, 7, 31),
                        initial_visible_month=datetime.date.today()
                    )
                ],
                style={'width': '32%', 'display': 'inline-block'}
            ),
            html.Div(
                [
                    html.H4('Students'),
                    dcc.Dropdown(
                        id='students-dropdown',
                        # options=[
                        #     {'label': 'Alpha', 'value': 'a'},
                        #     {'label': 'Beta', 'value': 'b'},
                        #     {'label': 'Gamma', 'value': 'c'}
                        # ],
                        # value=['a', 'c'],
                        multi=True
                    )
                ],
                style={'width': '32%', 'display': 'inline-block'}
            ),
            html.Div(
                [
                    html.H4('Materials'),
                    dcc.Dropdown(
                        id='materials-dropdown',
                        # options=[
                        #     {'label': 'One', 'value': '1'},
                        #     {'label': 'Two', 'value': '2'},
                        #     {'label': 'Three', 'value': '3'}
                        # ],
                        # value=[1],
                        multi=True
                    )
                ],
                style={'width': '32%', 'display': 'inline-block'}
            )
        ]),
        html.Div(
            dash_table.DataTable(
                id='table',
                columns=[
                    {"name": 'Student', "id": 'Student'},
                    {"name": 'Material', "id": 'Material'},
                    {"name": 'Day', "id": 'Day'},
                    {"name": 'Start', "id": 'Start'},
                    {"name": 'End', "id": 'End'}
                ],
                # filter_action='native',
                # filter_query='{Material} contains Bells && {Student} contains Flower Arranging',
                # fill_width=False,
                fixed_rows={'headers': True},
                page_action='none',
                style_table={'height': '500px', 'overflowY': 'auto'},
                style_as_list_view=True,
                style_data_conditional=[
                    {
                        'if': {'row_index': 'odd'},
                        'backgroundColor': 'rgb(248, 248, 248)'
                    }
                ],
                style_header={
                    'backgroundColor': 'rgb(230, 230, 230)',
                    'fontWeight': 'bold'
                },
                style_cell={
                    'whiteSpace': 'normal',
                    'height': 'auto',
                    'textAlign': 'left'
                }
            )
        )
    ])

app.layout = serve_layout

@app.callback(
    Output('table', "data"),
    Output('students-dropdown', "options"),
    Output('materials-dropdown', "options"),
    Input('session-id', "children"),
    Input('my-date-picker-range', "start_date"),
    Input('my-date-picker-range', "end_date"),
    Input('students-dropdown', "value"),
    Input('materials-dropdown', "value")
)
def update_data(
    session_id,
    start_date_string,
    end_date_string,
    selected_students,
    selected_materials
):
    if pd.isnull(start_date_string) or pd.isnull(end_date_string):
        return [], [], []
    material_interactions_display_df = fetch_dataframe(
        session_id,
        start_date_string,
        end_date_string
    )
    student_options = [
        {'label': option, 'value': option}
        for option in list(material_interactions_display_df['Student'].unique())
    ]
    material_options = [
        {'label': option, 'value': option}
        for option in list(material_interactions_display_df['Material'].unique())
    ]
    logger.info('Selected students: \'{}\''.format(selected_students))
    logger.info('Selected materials: \'{}\''.format(selected_materials))
    if selected_students is not None and len(selected_students) > 0:
        material_interactions_display_df = material_interactions_display_df.loc[
            material_interactions_display_df['Student'].isin(selected_students)
        ]
    if selected_materials is not None and len(selected_materials) > 0:
        material_interactions_display_df = material_interactions_display_df.loc[
            material_interactions_display_df['Material'].isin(selected_materials)
        ]
    table_data = material_interactions_display_df.to_dict('records')
    return table_data, student_options, material_options

def fetch_dataframe(
    session_id,
    start_date_string,
    end_date_string
):
    @cache.memoize()
    def fetch_and_serialize_data(
        session_id,
        start_date_string,
        end_date_string
    ):
        logger.info('Fetching new data from Honeycomb for start date {} and end date {}'.format(
            start_date_string,
            end_date_string
        ))
        if pd.isnull(start_date_string) or pd.isnull(end_date_string):
            return pd.DataFrame()
        start_date = pd.to_datetime(start_date_string).date()
        end_date = pd.to_datetime(end_date_string).date()
        # start_date = datetime.date(2021, 3, 29)
        # end_date = datetime.date(2021, 4, 2)
        start_hour = 0
        end_hour = 23
        time_zone_name = 'US/Central'
        start = datetime.datetime(
            year=start_date.year,
            month=start_date.month,
            day=start_date.day,
            hour=start_hour,
            tzinfo=dateutil.tz.gettz(time_zone_name)
        )
        end = datetime.datetime(
            year=end_date.year,
            month=end_date.month,
            day=end_date.day,
            hour=end_hour,
            tzinfo=dateutil.tz.gettz(time_zone_name)
        )
        material_interactions_df = honeycomb_io.fetch_material_interactions(
            start=start,
            end=end,
            material_interaction_ids=None,
            source_types=None,
            person_ids=None,
            material_ids=None,
            output_format='dataframe',
            chunk_size=1000,
            client=None,
            uri=None,
            token_uri=None,
            audience=None,
            client_id=None,
            client_secret=None
        )
        material_interactions_df.sort_values(
            'start',
            inplace=True
        )
        material_interactions_df['start_day'] = material_interactions_df['start'].apply(lambda x: local_day_format(x, time_zone_name))
        material_interactions_df['start_time'] = material_interactions_df['start'].apply(lambda x: local_time_format(x, time_zone_name))
        material_interactions_df['end_time'] = material_interactions_df['end'].apply(lambda x: local_time_format(x, time_zone_name))
        material_interactions_display_df = (
            material_interactions_df
            .reset_index()
            .fillna('')
            .reindex(columns=[
                'person_short_name',
                'material_name',
                'start_day',
                'start_time',
                'end_time'
            ])
            .rename(columns={
                'person_short_name': 'Student',
                'material_name': 'Material',
                'start_day': 'Day',
                'start_time': 'Start',
                'end_time': 'End'
            })
        )
        return material_interactions_display_df.to_json()
    return pd.read_json(fetch_and_serialize_data(
        session_id,
        start_date_string,
        end_date_string
    ))

def local_time_format(dt, time_zone_name):
    if pd.notnull(dt):
        return (
            dt
            .tz_convert(dateutil.tz.gettz(time_zone_name))
            .strftime('%I:%M %p')
        )
    else:
        return ''

def local_day_format(dt, time_zone_name):
    if pd.notnull(dt):
        return (
            dt
            .tz_convert(dateutil.tz.gettz(time_zone_name))
            .strftime('%a')
        )
    else:
        return ''


if __name__ == '__main__':
    app.run_server(debug=True)
