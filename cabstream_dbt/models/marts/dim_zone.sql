-- dim_zone.sql
-- 265 NYC taxi zones with borough mapping
-- Source: TLC taxi zone lookup (hardcoded known zones)
-- Why hardcode: zone lookup is static, never changes

with zones as (
    select * from (
        values
        (1,  'Newark Airport',           'EWR'),
        (2,  'Jamaica Bay',              'Queens'),
        (3,  'Allerton/Pelham Gardens',  'Bronx'),
        (4,  'Alphabet City',            'Manhattan'),
        (5,  'Arden Heights',            'Staten Island'),
        (6,  'Arrochar/Fort Wadsworth',  'Staten Island'),
        (7,  'Astoria',                  'Queens'),
        (8,  'Astoria Park',             'Queens'),
        (9,  'Auburndale',               'Queens'),
        (10, 'Baisley Park',             'Queens'),
        (11, 'Bath Beach',               'Brooklyn'),
        (12, 'Battery Park',             'Manhattan'),
        (13, 'Battery Park City',        'Manhattan'),
        (14, 'Bay Ridge',                'Brooklyn'),
        (15, 'Bay Terrace/Fort Totten',  'Queens'),
        (16, 'Bayside',                  'Queens'),
        (17, 'Bedford',                  'Brooklyn'),
        (18, 'Bedford Park',             'Bronx'),
        (19, 'Bellerose',                'Queens'),
        (20, 'Belmont',                  'Bronx'),
        (48, 'Clinton East',             'Manhattan'),
        (79, 'Flatbush/Ditmas Park',     'Brooklyn'),
        (80, 'Flushing',                 'Queens'),
        (107,'Kips Bay',                 'Manhattan'),
        (132,'JFK Airport',              'Queens'),
        (138,'LaGuardia Airport',        'Queens'),
        (140,'Lenox Hill East',          'Manhattan'),
        (141,'Lenox Hill West',          'Manhattan'),
        (142,'Lincoln Square East',      'Manhattan'),
        (143,'Lincoln Square West',      'Manhattan'),
        (144,'Little Italy/NoLiTa',      'Manhattan'),
        (145,'Long Island City/Hunters Point','Queens'),
        (148,'Lower East Side',          'Manhattan'),
        (151,'Manhattan Valley',         'Manhattan'),
        (152,'Manhattanville',           'Manhattan'),
        (153,'Marble Hill/Inwood',       'Manhattan'),
        (158,'Meatpacking/West Village West','Manhattan'),
        (161,'Midtown Center',           'Manhattan'),
        (162,'Midtown East',             'Manhattan'),
        (163,'Midtown North',            'Manhattan'),
        (164,'Midtown South',            'Manhattan'),
        (166,'Morningside Heights',      'Manhattan'),
        (170,'Murray Hill',              'Manhattan'),
        (186,'Penn Station/Madison Sq West','Manhattan'),
        (194,'Rockefeller Center',       'Manhattan'),
        (202,'Sheepshead Bay',           'Brooklyn'),
        (209,'Soho',                     'Manhattan'),
        (211,'Stuy Town/Peter Cooper Village','Manhattan'),
        (224,'Times Sq/Theatre District','Manhattan'),
        (229,'Turtle Bay/East Midtown',  'Manhattan'),
        (230,'UN/Turtle Bay South',      'Manhattan'),
        (231,'Union Sq',                 'Manhattan'),
        (232,'Upper East Side North',    'Manhattan'),
        (233,'Upper East Side South',    'Manhattan'),
        (234,'Upper West Side North',    'Manhattan'),
        (235,'Upper West Side South',    'Manhattan'),
        (236,'Van Cortlandt Park',       'Bronx'),
        (237,'Van Cortlandt Village',    'Bronx'),
        (238,'Van Nest/Morris Park',     'Bronx'),
        (239,'Washington Heights North', 'Manhattan'),
        (240,'Washington Heights South', 'Manhattan'),
        (243,'West Chelsea/Hudson Yards','Manhattan'),
        (244,'West Concourse',           'Bronx'),
        (246,'West Farms/Bronx River',   'Bronx'),
        (247,'West Village',             'Manhattan'),
        (248,'Westchester Village/Unionport','Bronx'),
        (249,'Westies',                  'Manhattan'),
        (261,'World Trade Center',       'Manhattan'),
        (262,'Yorkville East',           'Manhattan'),
        (263,'Yorkville West',           'Manhattan'),
        (264,'NV',                       'Unknown'),
        (265,'NA',                       'Unknown')
    ) as t(zone_id, zone_name, borough)
)

select
    zone_id,
    zone_name,
    borough,
    case borough
        when 'Manhattan'    then 1
        when 'Brooklyn'     then 2
        when 'Queens'       then 3
        when 'Bronx'        then 4
        when 'Staten Island' then 5
        else 6
    end as borough_id
from zones