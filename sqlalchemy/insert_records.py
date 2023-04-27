"""Program to perform the insertion of the record(s) of the table

Source:
    https://python.plainenglish.io/generating-a-fake-database-with-python-8523bf6db9ec
    https://seraph13.medium.com/generar-datos-falsos-con-faker-4d4313ff8205
"""

from collections import OrderedDict
from faker import Faker
from sqlalchemy import create_engine, desc, MetaData,\
    insert, select
import os
import pyodbc
import urllib
import random
import sys


# locales faker object
locales = OrderedDict([
    ('es_AR', 1),
    ('es_CL', 2),
    ('es_MX', 3)
])
# Instantiate faker object
fake = Faker(locales)

# Define full path for database file
DB_DRIVER = "{Microsoft Access Driver (*.mdb, *.accdb)}"
DB_PATH = os.path.dirname(
    os.path.abspath(__file__)
) + os.sep + "data" + os.sep
DB_FILE = 'database.accdb'
DB = DB_PATH + DB_FILE

# Make DNS string
CONNECTION_STRING = (
    r'DRIVER={0};'
    r'DBQ={1};'
    'ExtendedAnsiSQL=1;'
).format(DB_DRIVER, DB)

# Set up connections between sqlalchemy and access+pyodbc
# Instantiate metadate object
engine = create_engine(f"access+pyodbc:///?odbc_connect={urllib.parse.quote(CONNECTION_STRING)}")
metadata = MetaData()

# Reflect metadata/schema from existing database to bring in existing tables
with engine.connect() as conn:
    metadata.reflect(conn)

estados = metadata.tables["estados"]
ciudades = metadata.tables["ciudades"]
categorias = metadata.tables["categorias"]
productos = metadata.tables["productos"]
clientes = metadata.tables["clientes"]
pedidos = metadata.tables["pedidos"]

# 'estados' list
ESTADOS_MULTIPLE_ROWS = [
    (1, 'Amazonas', 'VE-X'),
    (2, 'Anzoátegui', 'VE-B'),
    (3, 'Apure', 'VE-C'),
    (4, 'Aragua', 'VE-D'),
    (5, 'Barinas', 'VE-E'),
    (6, 'Bolívar', 'VE-F'),
    (7, 'Carabobo', 'VE-G'),
    (8, 'Cojedes', 'VE-H'),
    (9, 'Delta Amacuro', 'VE-Y'),
    (10, 'Falcón', 'VE-I'),
    (11, 'Guárico', 'VE-J'),
    (12, 'Lara', 'VE-K'),
    (13, 'Mérida', 'VE-L'),
    (14, 'Miranda', 'VE-M'),
    (15, 'Monagas', 'VE-N'),
    (16, 'Nueva Esparta', 'VE-O'),
    (17, 'Portuguesa', 'VE-P'),
    (18, 'Sucre', 'VE-R'),
    (19, 'Táchira', 'VE-S'),
    (20, 'Trujillo', 'VE-T'),
    (21, 'La Guaira', 'VE-W'),
    (22, 'Yaracuy', 'VE-U'),
    (23, 'Zulia', 'VE-V'),
    (24, 'Distrito Capital', 'VE-A'),
    (25, 'Dependencias Federales', 'VE-Z')
]

# 'ciudades' list
CIUDADES_MULTIPLE_ROWS = [
    (1, 1, 'Maroa', 0),
    (2, 1, 'Puerto Ayacucho', 1),
    (3, 1, 'San Fernando de Atabapo', 0),
    (4, 2, 'Anaco', 0),
    (5, 2, 'Aragua de Barcelona', 0),
    (6, 2, 'Barcelona', 1),
    (7, 2, 'Boca de Uchire', 0),
    (8, 2, 'Cantaura', 0),
    (9, 2, 'Clarines', 0),
    (10, 2, 'El Chaparro', 0),
    (11, 2, 'El Pao Anzoátegui', 0),
    (12, 2, 'El Tigre', 0),
    (13, 2, 'El Tigrito', 0),
    (14, 2, 'Guanape', 0),
    (15, 2, 'Guanta', 0),
    (16, 2, 'Lechería', 0),
    (17, 2, 'Onoto', 0),
    (18, 2, 'Pariaguán', 0),
    (19, 2, 'Píritu', 0),
    (20, 2, 'Puerto La Cruz', 0),
    (21, 2, 'Puerto Píritu', 0),
    (22, 2, 'Sabana de Uchire', 0),
    (23, 2, 'San Mateo Anzoátegui', 0),
    (24, 2, 'San Pablo Anzoátegui', 0),
    (25, 2, 'San Tomé', 0),
    (26, 2, 'Santa Ana de Anzoátegui', 0),
    (27, 2, 'Santa Fe Anzoátegui', 0),
    (28, 2, 'Santa Rosa', 0),
    (29, 2, 'Soledad', 0),
    (30, 2, 'Urica', 0),
    (31, 2, 'Valle de Guanape', 0),
    (43, 3, 'Achaguas', 0),
    (44, 3, 'Biruaca', 0),
    (45, 3, 'Bruzual', 0),
    (46, 3, 'El Amparo', 0),
    (47, 3, 'El Nula', 0),
    (48, 3, 'Elorza', 0),
    (49, 3, 'Guasdualito', 0),
    (50, 3, 'Mantecal', 0),
    (51, 3, 'Puerto Páez', 0),
    (52, 3, 'San Fernando de Apure', 1),
    (53, 3, 'San Juan de Payara', 0),
    (54, 4, 'Barbacoas', 0),
    (55, 4, 'Cagua', 0),
    (56, 4, 'Camatagua', 0),
    (58, 4, 'Choroní', 0),
    (59, 4, 'Colonia Tovar', 0),
    (60, 4, 'El Consejo', 0),
    (61, 4, 'La Victoria', 0),
    (62, 4, 'Las Tejerías', 0),
    (63, 4, 'Magdaleno', 0),
    (64, 4, 'Maracay', 1),
    (65, 4, 'Ocumare de La Costa', 0),
    (66, 4, 'Palo Negro', 0),
    (67, 4, 'San Casimiro', 0),
    (68, 4, 'San Mateo', 0),
    (69, 4, 'San Sebastián', 0),
    (70, 4, 'Santa Cruz de Aragua', 0),
    (71, 4, 'Tocorón', 0),
    (72, 4, 'Turmero', 0),
    (73, 4, 'Villa de Cura', 0),
    (74, 4, 'Zuata', 0),
    (75, 5, 'Barinas', 1),
    (76, 5, 'Barinitas', 0),
    (77, 5, 'Barrancas', 0),
    (78, 5, 'Calderas', 0),
    (79, 5, 'Capitanejo', 0),
    (80, 5, 'Ciudad Bolivia', 0),
    (81, 5, 'El Cantón', 0),
    (82, 5, 'Las Veguitas', 0),
    (83, 5, 'Libertad de Barinas', 0),
    (84, 5, 'Sabaneta', 0),
    (85, 5, 'Santa Bárbara de Barinas', 0),
    (86, 5, 'Socopó', 0),
    (87, 6, 'Caicara del Orinoco', 0),
    (88, 6, 'Canaima', 0),
    (89, 6, 'Ciudad Bolívar', 1),
    (90, 6, 'Ciudad Piar', 0),
    (91, 6, 'El Callao', 0),
    (92, 6, 'El Dorado', 0),
    (93, 6, 'El Manteco', 0),
    (94, 6, 'El Palmar', 0),
    (95, 6, 'El Pao', 0),
    (96, 6, 'Guasipati', 0),
    (97, 6, 'Guri', 0),
    (98, 6, 'La Paragua', 0),
    (99, 6, 'Matanzas', 0),
    (100, 6, 'Puerto Ordaz', 0),
    (101, 6, 'San Félix', 0),
    (102, 6, 'Santa Elena de Uairén', 0),
    (103, 6, 'Tumeremo', 0),
    (104, 6, 'Unare', 0),
    (105, 6, 'Upata', 0),
    (106, 7, 'Bejuma', 0),
    (107, 7, 'Belén', 0),
    (108, 7, 'Campo de Carabobo', 0),
    (109, 7, 'Canoabo', 0),
    (110, 7, 'Central Tacarigua', 0),
    (111, 7, 'Chirgua', 0),
    (112, 7, 'Ciudad Alianza', 0),
    (113, 7, 'El Palito', 0),
    (114, 7, 'Guacara', 0),
    (115, 7, 'Guigue', 0),
    (116, 7, 'Las Trincheras', 0),
    (117, 7, 'Los Guayos', 0),
    (118, 7, 'Mariara', 0),
    (119, 7, 'Miranda', 0),
    (120, 7, 'Montalbán', 0),
    (121, 7, 'Morón', 0),
    (122, 7, 'Naguanagua', 0),
    (123, 7, 'Puerto Cabello', 0),
    (124, 7, 'San Joaquín', 0),
    (125, 7, 'Tocuyito', 0),
    (126, 7, 'Urama', 0),
    (127, 7, 'Valencia', 1),
    (128, 7, 'Vigirimita', 0),
    (129, 8, 'Aguirre', 0),
    (130, 8, 'Apartaderos Cojedes', 0),
    (131, 8, 'Arismendi', 0),
    (132, 8, 'Camuriquito', 0),
    (133, 8, 'El Baúl', 0),
    (134, 8, 'El Limón', 0),
    (135, 8, 'El Pao Cojedes', 0),
    (136, 8, 'El Socorro', 0),
    (137, 8, 'La Aguadita', 0),
    (138, 8, 'Las Vegas', 0),
    (139, 8, 'Libertad de Cojedes', 0),
    (140, 8, 'Mapuey', 0),
    (141, 8, 'Piñedo', 0),
    (142, 8, 'Samancito', 0),
    (143, 8, 'San Carlos', 1),
    (144, 8, 'Sucre', 0),
    (145, 8, 'Tinaco', 0),
    (146, 8, 'Tinaquillo', 0),
    (147, 8, 'Vallecito', 0),
    (148, 9, 'Tucupita', 1),
    (149, 24, 'Caracas', 1),
    (150, 24, 'El Junquito', 0),
    (151, 10, 'Adícora', 0),
    (152, 10, 'Boca de Aroa', 0),
    (153, 10, 'Cabure', 0),
    (154, 10, 'Capadare', 0),
    (155, 10, 'Capatárida', 0),
    (156, 10, 'Chichiriviche', 0),
    (157, 10, 'Churuguara', 0),
    (158, 10, 'Coro', 1),
    (159, 10, 'Cumarebo', 0),
    (160, 10, 'Dabajuro', 0),
    (161, 10, 'Judibana', 0),
    (162, 10, 'La Cruz de Taratara', 0),
    (163, 10, 'La Vela de Coro', 0),
    (164, 10, 'Los Taques', 0),
    (165, 10, 'Maparari', 0),
    (166, 10, 'Mene de Mauroa', 0),
    (167, 10, 'Mirimire', 0),
    (168, 10, 'Pedregal', 0),
    (169, 10, 'Píritu Falcón', 0),
    (170, 10, 'Pueblo Nuevo Falcón', 0),
    (171, 10, 'Puerto Cumarebo', 0),
    (172, 10, 'Punta Cardón', 0),
    (173, 10, 'Punto Fijo', 0),
    (174, 10, 'San Juan de Los Cayos', 0),
    (175, 10, 'San Luis', 0),
    (176, 10, 'Santa Ana Falcón', 0),
    (177, 10, 'Santa Cruz De Bucaral', 0),
    (178, 10, 'Tocopero', 0),
    (179, 10, 'Tocuyo de La Costa', 0),
    (180, 10, 'Tucacas', 0),
    (181, 10, 'Yaracal', 0),
    (182, 11, 'Altagracia de Orituco', 0),
    (183, 11, 'Cabruta', 0),
    (184, 11, 'Calabozo', 0),
    (185, 11, 'Camaguán', 0),
    (196, 11, 'Chaguaramas Guárico', 0),
    (197, 11, 'El Socorro', 0),
    (198, 11, 'El Sombrero', 0),
    (199, 11, 'Las Mercedes de Los Llanos', 0),
    (200, 11, 'Lezama', 0),
    (201, 11, 'Onoto', 0),
    (202, 11, 'Ortíz', 0),
    (203, 11, 'San José de Guaribe', 0),
    (204, 11, 'San Juan de Los Morros', 1),
    (205, 11, 'San Rafael de Laya', 0),
    (206, 11, 'Santa María de Ipire', 0),
    (207, 11, 'Tucupido', 0),
    (208, 11, 'Valle de La Pascua', 0),
    (209, 11, 'Zaraza', 0),
    (210, 12, 'Aguada Grande', 0),
    (211, 12, 'Atarigua', 0),
    (212, 12, 'Barquisimeto', 1),
    (213, 12, 'Bobare', 0),
    (214, 12, 'Cabudare', 0),
    (215, 12, 'Carora', 0),
    (216, 12, 'Cubiro', 0),
    (217, 12, 'Cují', 0),
    (218, 12, 'Duaca', 0),
    (219, 12, 'El Manzano', 0),
    (220, 12, 'El Tocuyo', 0),
    (221, 12, 'Guaríco', 0),
    (222, 12, 'Humocaro Alto', 0),
    (223, 12, 'Humocaro Bajo', 0),
    (224, 12, 'La Miel', 0),
    (225, 12, 'Moroturo', 0),
    (226, 12, 'Quíbor', 0),
    (227, 12, 'Río Claro', 0),
    (228, 12, 'Sanare', 0),
    (229, 12, 'Santa Inés', 0),
    (230, 12, 'Sarare', 0),
    (231, 12, 'Siquisique', 0),
    (232, 12, 'Tintorero', 0),
    (233, 13, 'Apartaderos Mérida', 0),
    (234, 13, 'Arapuey', 0),
    (235, 13, 'Bailadores', 0),
    (236, 13, 'Caja Seca', 0),
    (237, 13, 'Canaguá', 0),
    (238, 13, 'Chachopo', 0),
    (239, 13, 'Chiguara', 0),
    (240, 13, 'Ejido', 0),
    (241, 13, 'El Vigía', 0),
    (242, 13, 'La Azulita', 0),
    (243, 13, 'La Playa', 0),
    (244, 13, 'Lagunillas Mérida', 0),
    (245, 13, 'Mérida', 1),
    (246, 13, 'Mesa de Bolívar', 0),
    (247, 13, 'Mucuchíes', 0),
    (248, 13, 'Mucujepe', 0),
    (249, 13, 'Mucuruba', 0),
    (250, 13, 'Nueva Bolivia', 0),
    (251, 13, 'Palmarito', 0),
    (252, 13, 'Pueblo Llano', 0),
    (253, 13, 'Santa Cruz de Mora', 0),
    (254, 13, 'Santa Elena de Arenales', 0),
    (255, 13, 'Santo Domingo', 0),
    (256, 13, 'Tabáy', 0),
    (257, 13, 'Timotes', 0),
    (258, 13, 'Torondoy', 0),
    (259, 13, 'Tovar', 0),
    (260, 13, 'Tucani', 0),
    (261, 13, 'Zea', 0),
    (262, 14, 'Araguita', 0),
    (263, 14, 'Carrizal', 0),
    (264, 14, 'Caucagua', 0),
    (265, 14, 'Chaguaramas Miranda', 0),
    (266, 14, 'Charallave', 0),
    (267, 14, 'Chirimena', 0),
    (268, 14, 'Chuspa', 0),
    (269, 14, 'Cúa', 0),
    (270, 14, 'Cupira', 0),
    (271, 14, 'Curiepe', 0),
    (272, 14, 'El Guapo', 0),
    (273, 14, 'El Jarillo', 0),
    (274, 14, 'Filas de Mariche', 0),
    (275, 14, 'Guarenas', 0),
    (276, 14, 'Guatire', 0),
    (277, 14, 'Higuerote', 0),
    (278, 14, 'Los Anaucos', 0),
    (279, 14, 'Los Teques', 1),
    (280, 14, 'Ocumare del Tuy', 0),
    (281, 14, 'Panaquire', 0),
    (282, 14, 'Paracotos', 0),
    (283, 14, 'Río Chico', 0),
    (284, 14, 'San Antonio de Los Altos', 0),
    (285, 14, 'San Diego de Los Altos', 0),
    (286, 14, 'San Fernando del Guapo', 0),
    (287, 14, 'San Francisco de Yare', 0),
    (288, 14, 'San José de Los Altos', 0),
    (289, 14, 'San José de Río Chico', 0),
    (290, 14, 'San Pedro de Los Altos', 0),
    (291, 14, 'Santa Lucía', 0),
    (292, 14, 'Santa Teresa', 0),
    (293, 14, 'Tacarigua de La Laguna', 0),
    (294, 14, 'Tacarigua de Mamporal', 0),
    (295, 14, 'Tácata', 0),
    (296, 14, 'Turumo', 0),
    (297, 15, 'Aguasay', 0),
    (298, 15, 'Aragua de Maturín', 0),
    (299, 15, 'Barrancas del Orinoco', 0),
    (300, 15, 'Caicara de Maturín', 0),
    (301, 15, 'Caripe', 0),
    (302, 15, 'Caripito', 0),
    (303, 15, 'Chaguaramal', 0),
    (305, 15, 'Chaguaramas Monagas', 0),
    (307, 15, 'El Furrial', 0),
    (308, 15, 'El Tejero', 0),
    (309, 15, 'Jusepín', 0),
    (310, 15, 'La Toscana', 0),
    (311, 15, 'Maturín', 1),
    (312, 15, 'Miraflores', 0),
    (313, 15, 'Punta de Mata', 0),
    (314, 15, 'Quiriquire', 0),
    (315, 15, 'San Antonio de Maturín', 0),
    (316, 15, 'San Vicente Monagas', 0),
    (317, 15, 'Santa Bárbara', 0),
    (318, 15, 'Temblador', 0),
    (319, 15, 'Teresen', 0),
    (320, 15, 'Uracoa', 0),
    (321, 16, 'Altagracia', 0),
    (322, 16, 'Boca de Pozo', 0),
    (323, 16, 'Boca de Río', 0),
    (324, 16, 'El Espinal', 0),
    (325, 16, 'El Valle del Espíritu Santo', 0),
    (326, 16, 'El Yaque', 0),
    (327, 16, 'Juangriego', 0),
    (328, 16, 'La Asunción', 1),
    (329, 16, 'La Guardia', 0),
    (330, 16, 'Pampatar', 0),
    (331, 16, 'Porlamar', 0),
    (332, 16, 'Puerto Fermín', 0),
    (333, 16, 'Punta de Piedras', 0),
    (334, 16, 'San Francisco de Macanao', 0),
    (335, 16, 'San Juan Bautista', 0),
    (336, 16, 'San Pedro de Coche', 0),
    (337, 16, 'Santa Ana de Nueva Esparta', 0),
    (338, 16, 'Villa Rosa', 0),
    (339, 17, 'Acarigua', 0),
    (340, 17, 'Agua Blanca', 0),
    (341, 17, 'Araure', 0),
    (342, 17, 'Biscucuy', 0),
    (343, 17, 'Boconoito', 0),
    (344, 17, 'Campo Elías', 0),
    (345, 17, 'Chabasquén', 0),
    (346, 17, 'Guanare', 1),
    (347, 17, 'Guanarito', 0),
    (348, 17, 'La Aparición', 0),
    (349, 17, 'La Misión', 0),
    (350, 17, 'Mesa de Cavacas', 0),
    (351, 17, 'Ospino', 0),
    (352, 17, 'Papelón', 0),
    (353, 17, 'Payara', 0),
    (354, 17, 'Pimpinela', 0),
    (355, 17, 'Píritu de Portuguesa', 0),
    (356, 17, 'San Rafael de Onoto', 0),
    (357, 17, 'Santa Rosalía', 0),
    (358, 17, 'Turén', 0),
    (359, 18, 'Altos de Sucre', 0),
    (360, 18, 'Araya', 0),
    (361, 18, 'Cariaco', 0),
    (362, 18, 'Carúpano', 0),
    (363, 18, 'Casanay', 0),
    (364, 18, 'Cumaná', 1),
    (365, 18, 'Cumanacoa', 0),
    (366, 18, 'El Morro Puerto Santo', 0),
    (367, 18, 'El Pilar', 0),
    (368, 18, 'El Poblado', 0),
    (369, 18, 'Guaca', 0),
    (370, 18, 'Guiria', 0),
    (371, 18, 'Irapa', 0),
    (372, 18, 'Manicuare', 0),
    (373, 18, 'Mariguitar', 0),
    (374, 18, 'Río Caribe', 0),
    (375, 18, 'San Antonio del Golfo', 0),
    (376, 18, 'San José de Aerocuar', 0),
    (377, 18, 'San Vicente de Sucre', 0),
    (378, 18, 'Santa Fe de Sucre', 0),
    (379, 18, 'Tunapuy', 0),
    (380, 18, 'Yaguaraparo', 0),
    (381, 18, 'Yoco', 0),
    (382, 19, 'Abejales', 0),
    (383, 19, 'Borota', 0),
    (384, 19, 'Bramon', 0),
    (385, 19, 'Capacho', 0),
    (386, 19, 'Colón', 0),
    (387, 19, 'Coloncito', 0),
    (388, 19, 'Cordero', 0),
    (389, 19, 'El Cobre', 0),
    (390, 19, 'El Pinal', 0),
    (391, 19, 'Independencia', 0),
    (392, 19, 'La Fría', 0),
    (393, 19, 'La Grita', 0),
    (394, 19, 'La Pedrera', 0),
    (395, 19, 'La Tendida', 0),
    (396, 19, 'Las Delicias', 0),
    (397, 19, 'Las Hernández', 0),
    (398, 19, 'Lobatera', 0),
    (399, 19, 'Michelena', 0),
    (400, 19, 'Palmira', 0),
    (401, 19, 'Pregonero', 0),
    (402, 19, 'Queniquea', 0),
    (403, 19, 'Rubio', 0),
    (404, 19, 'San Antonio del Tachira', 0),
    (405, 19, 'San Cristobal', 1),
    (406, 19, 'San José de Bolívar', 0),
    (407, 19, 'San Josecito', 0),
    (408, 19, 'San Pedro del Río', 0),
    (409, 19, 'Santa Ana Táchira', 0),
    (410, 19, 'Seboruco', 0),
    (411, 19, 'Táriba', 0),
    (412, 19, 'Umuquena', 0),
    (413, 19, 'Ureña', 0),
    (414, 20, 'Batatal', 0),
    (415, 20, 'Betijoque', 0),
    (416, 20, 'Boconó', 0),
    (417, 20, 'Carache', 0),
    (418, 20, 'Chejende', 0),
    (419, 20, 'Cuicas', 0),
    (420, 20, 'El Dividive', 0),
    (421, 20, 'El Jaguito', 0),
    (422, 20, 'Escuque', 0),
    (423, 20, 'Isnotú', 0),
    (424, 20, 'Jajó', 0),
    (425, 20, 'La Ceiba', 0),
    (426, 20, 'La Concepción de Trujllo', 0),
    (427, 20, 'La Mesa de Esnujaque', 0),
    (428, 20, 'La Puerta', 0),
    (429, 20, 'La Quebrada', 0),
    (430, 20, 'Mendoza Fría', 0),
    (431, 20, 'Meseta de Chimpire', 0),
    (432, 20, 'Monay', 0),
    (433, 20, 'Motatán', 0),
    (434, 20, 'Pampán', 0),
    (435, 20, 'Pampanito', 0),
    (436, 20, 'Sabana de Mendoza', 0),
    (437, 20, 'San Lázaro', 0),
    (438, 20, 'Santa Ana de Trujillo', 0),
    (439, 20, 'Tostós', 0),
    (440, 20, 'Trujillo', 1),
    (441, 20, 'Valera', 0),
    (442, 21, 'Carayaca', 0),
    (443, 21, 'Litoral', 0),
    (444, 25, 'Archipiélago Los Roques', 0),
    (445, 22, 'Aroa', 0),
    (446, 22, 'Boraure', 0),
    (447, 22, 'Campo Elías de Yaracuy', 0),
    (448, 22, 'Chivacoa', 0),
    (449, 22, 'Cocorote', 0),
    (450, 22, 'Farriar', 0),
    (451, 22, 'Guama', 0),
    (452, 22, 'Marín', 0),
    (453, 22, 'Nirgua', 0),
    (454, 22, 'Sabana de Parra', 0),
    (455, 22, 'Salom', 0),
    (456, 22, 'San Felipe', 1),
    (457, 22, 'San Pablo de Yaracuy', 0),
    (458, 22, 'Urachiche', 0),
    (459, 22, 'Yaritagua', 0),
    (460, 22, 'Yumare', 0),
    (461, 23, 'Bachaquero', 0),
    (462, 23, 'Bobures', 0),
    (463, 23, 'Cabimas', 0),
    (464, 23, 'Campo Concepción', 0),
    (465, 23, 'Campo Mara', 0),
    (466, 23, 'Campo Rojo', 0),
    (467, 23, 'Carrasquero', 0),
    (468, 23, 'Casigua', 0),
    (469, 23, 'Chiquinquirá', 0),
    (470, 23, 'Ciudad Ojeda', 0),
    (471, 23, 'El Batey', 0),
    (472, 23, 'El Carmelo', 0),
    (473, 23, 'El Chivo', 0),
    (474, 23, 'El Guayabo', 0),
    (475, 23, 'El Mene', 0),
    (476, 23, 'El Venado', 0),
    (477, 23, 'Encontrados', 0),
    (478, 23, 'Gibraltar', 0),
    (479, 23, 'Isla de Toas', 0),
    (480, 23, 'La Concepción del Zulia', 0),
    (481, 23, 'La Paz', 0),
    (482, 23, 'La Sierrita', 0),
    (483, 23, 'Lagunillas del Zulia', 0),
    (484, 23, 'Las Piedras de Perijá', 0),
    (485, 23, 'Los Cortijos', 0),
    (486, 23, 'Machiques', 0),
    (487, 23, 'Maracaibo', 1),
    (488, 23, 'Mene Grande', 0),
    (489, 23, 'Palmarejo', 0),
    (490, 23, 'Paraguaipoa', 0),
    (491, 23, 'Potrerito', 0),
    (492, 23, 'Pueblo Nuevo del Zulia', 0),
    (493, 23, 'Puertos de Altagracia', 0),
    (494, 23, 'Punta Gorda', 0),
    (495, 23, 'Sabaneta de Palma', 0),
    (496, 23, 'San Francisco', 0),
    (497, 23, 'San José de Perijá', 0),
    (498, 23, 'San Rafael del Moján', 0),
    (499, 23, 'San Timoteo', 0),
    (500, 23, 'Santa Bárbara Del Zulia', 0),
    (501, 23, 'Santa Cruz de Mara', 0),
    (502, 23, 'Santa Cruz del Zulia', 0),
    (503, 23, 'Santa Rita', 0),
    (504, 23, 'Sinamaica', 0),
    (505, 23, 'Tamare', 0),
    (506, 23, 'Tía Juana', 0),
    (507, 23, 'Villa del Rosario', 0),
    (508, 21, 'La Guaira', 1),
    (509, 21, 'Catia La Mar', 0),
    (510, 21, 'Macuto', 0),
    (511, 21, 'Naiguatá', 0),
    (512, 25, 'Archipiélago Los Monjes', 0),
    (513, 25, 'Isla La Tortuga y Cayos adyacentes', 0),
    (514, 25, 'Isla La Sola', 0),
    (515, 25, 'Islas Los Testigos', 0),
    (516, 25, 'Islas Los Frailes', 0),
    (517, 25, 'Isla La Orchila', 0),
    (518, 25, 'Archipiélago Las Aves', 0),
    (519, 25, 'Isla de Aves', 0),
    (520, 25, 'Isla La Blanquilla', 0),
    (521, 25, 'Isla de Patos', 0),
    (522, 25, 'Islas Los Hermanos', 0)
]

# 'categorias' list
CATEGORIAS_MULTIPLE_ROWS = [
    (1, 'Tecnología', True),
    (2, 'Ropa', False),
    (3, 'Estética', True),
    (4, 'Herramientas', True),
    (5, 'Entretenimientos', True)
]

# 'productos' list
PRODUCTOS_MULTIPLE_ROWS = [
    (1, "Pantalón Jean LEVI'S 511", "Pantalón Jean LEVI'S 511 Slim Fit, Talla 34x32 y 34x34, Color disponible Pumped Up, 99% Algodón y 1% Elastane, Hecho en Bangladesh. Producto 100% original, Producto importado de EE.UU. Envíos a todo el país.", 2, 59.33, True),
    (2, 'Teléfono iPhone 13 Pro Max', 'Producto 2 detallado', 1, 1045.56, False),
    (3, 'Consola Play Station 5', 'Consola Play Station 5; Edición Gob Of War; Capacidad 825 GB; Memoria RAM de 16 GB; Tipo de consola de sobremesa, Wi-FI incluido; cantidad de controles incluidos 1. Producto 100% original, Producto importado de EE.UU. Envíos a todo el país.', 5, 829.00, True),
    (4, 'Caja de herramientas Stanley 99 piezas', 'Caja de herramientas Stanley 99 piezas; Caja de herramientas Stanley, Cantidad de piezas incluidas 99 como dados, extensiones, destornillador y llaves. Producto 100% original, Producto importado de EE.UU. Envíos a todo el país.', 4, 180.30, True),
    (5, 'Zapatos Clarks', 'Zapatos Clarks; Talla 39, 40 y 41; Colores disponible  Negro, Marrón, Azul; 99% Cuero 1% Tela, Hecho en Londres. Producto 100% original, Producto importado de Inglaterra. Envíos a todo el país.', 2, 120.00, True)
]

# 'clientes' list
CLIENTES_MULTIPLE_ROWS = [
    (1, 'Leonardo', 'Caballero', 245, '04144567239'),
    (2, 'Ana', 'Poleo', 6, '04249804536'),
    (3, 'Rafa', 'Lugo', 461, '04121894605'),
    (4, 'Eduardo', 'Perez', 487, '04163387633'),
    (5, 'Maximiliano', 'Vilchez', 245, '04264893321')
]

# 'pedidos' list
PEDIDOS_MULTIPLE_ROWS = [
    (1, 1, '12/02/2022 10:23:45 AM', 2, True),
    (2, 3, '01/06/2023 03:55:51 PM', 1, True),
    (3, 4, '02/18/2023 12:48:33 AM', 2, True),
    (4, 5, '03/22/2023 07:22:03 PM', 1, True),
    (5, 2, '03/12/2023 08:26:54 PM', 3, True)
]


class GenerateData:
    """Generate a specific number of records to a target table in
    the database"""

    def __init__(self, table="", numbers=0):
        """Initialize command line arguments

        Args:
            table (str, optional): The table name to manipulate. Defaults to "".
            numbers (int, optional): The rows number to insert into the table. Defaults to 0.
        """
        self.table_name = table
        self.num_records = numbers


    def __str__(self):
        """Informal Representation Class"""
        return f"Table: '{self.table_name}' and Number Records: '{self.num_records}'."


    def __repr__(self):
        """Official Representation Class"""
        return f"Table: '{self.table_name}'."

    def capitalize_list(self, list=[]):
        """Capitalize the values of a list

        Args:
            list (list, optional): A list values for Capitalize. Defaults to [].

        Returns:
            list: A list values capitalized
        """
        return [l.capitalize() for l in list]


    def create_data(self):
        """Using faker library, generate data and execute DML"""

        if self.table_name not in metadata.tables.keys():
            return print(f"{self.table_name} does not exist")

        if self.table_name == "estados" and self.num_records == 0:
            with engine.begin() as conn:
                for fila in ESTADOS_MULTIPLE_ROWS:
                    insert_stmt = estados.insert().values(
                        id=fila[0],
                        nombre=fila[1],
                        iso_3166_2=fila[2],
                    )
                    conn.execute(insert_stmt)
                print(f"\n'{len(ESTADOS_MULTIPLE_ROWS)}' row(s) inserted into '{self.table_name}' table!")
        else:
            pass

        if self.table_name == "ciudades" and self.num_records == 0:
            with engine.begin() as conn:
                for fila in CIUDADES_MULTIPLE_ROWS:
                    insert_stmt = ciudades.insert().values(
                        id=fila[0],
                        estado_id=fila[1],
                        nombre=fila[2],
                        capital=fila[3],
                    )
                    conn.execute(insert_stmt)
                print(f"\n'{len(CIUDADES_MULTIPLE_ROWS)}' row(s) inserted into '{self.table_name}' table!")
        else:
            pass

        if self.table_name == "categorias" and self.num_records == 0:
            with engine.begin() as conn:
                for fila in CATEGORIAS_MULTIPLE_ROWS:
                    insert_stmt = categorias.insert().values(
                        id=fila[0],
                        nombre=fila[1],
                        status=fila[2],
                    )
                    conn.execute(insert_stmt)
                print(f"\n'{len(CATEGORIAS_MULTIPLE_ROWS)}' row(s) inserted into '{self.table_name}' table!")
        else:
            pass

        if self.table_name == "productos" and self.num_records == 0:
            with engine.begin() as conn:
                for fila in PRODUCTOS_MULTIPLE_ROWS:
                    insert_stmt = productos.insert().values(
                        id=fila[0],
                        nombre=fila[1],
                        descripcion=fila[2],
                        categoria_id=fila[3],
                        precio=fila[4],
                        status=fila[5],
                    )
                    conn.execute(insert_stmt)
                print(f"\n'{len(PRODUCTOS_MULTIPLE_ROWS)}' row(s) inserted into '{self.table_name}' table!")
        elif self.table_name == "productos" and self.num_records > 0:
            with engine.begin() as conn:
                for _ in range(self.num_records):
                    insert_stmt = productos.insert().values(
                        id=int(
                            conn.execute(
                                productos.select().order_by(
                                    desc(productos.c.id)
                                )
                            ).first()[0]
                        ) + 1,
                        nombre=" ".join(self.capitalize_list(list(fake.words()))),
                        descripcion=fake.sentence(nb_words=10),
                        categoria_id=random.choice(
                            conn.execute(
                                categorias.select()
                            ).fetchall()
                        )[0],
                        precio=fake.random_int(1,100000) / 100.0,
                        status=random.choice([True, False]),
                    )
                    conn.execute(insert_stmt)
                print(f"\n'{self.num_records}' row(s) inserted into '{self.table_name}' table!")
        else:
            pass

        if self.table_name == "clientes" and self.num_records == 0:
            with engine.begin() as conn:
                for fila in CLIENTES_MULTIPLE_ROWS:
                    insert_stmt = clientes.insert().values(
                        id=fila[0],
                        nombre=fila[1],
                        apellido=fila[2],
                        codigo_postal=fila[3],
                        telefono=fila[4],
                    )
                    conn.execute(insert_stmt)
                print(f"\n'{len(CLIENTES_MULTIPLE_ROWS)}' row(s) inserted into '{self.table_name}' table!")
        elif self.table_name == "clientes" and self.num_records > 0:
            with engine.begin() as conn:
                for _ in range(self.num_records):
                    insert_stmt = clientes.insert().values(
                        id=int(
                            conn.execute(
                                clientes.select().order_by(
                                    desc(clientes.c.id)
                                )
                            ).first()[0]
                        ) + 1,
                        nombre=fake.first_name(),
                        apellido=fake.last_name(),
                        codigo_postal=random.choice(
                            conn.execute(
                                ciudades.select()
                            ).fetchall()
                        )[0],
                        telefono=fake.unique.phone_number(),
                    )
                    conn.execute(insert_stmt)
                print(f"\n'{self.num_records}' row(s) inserted into '{self.table_name}' table!")
        else:
            pass

        if self.table_name == "pedidos" and self.num_records == 0:
            with engine.begin() as conn:
                for fila in PEDIDOS_MULTIPLE_ROWS:
                    insert_stmt = pedidos.insert().values(
                        id=fila[0],
                        cliente_id=fila[1],
                        fecha=fila[2],
                        producto_id=fila[3],
                        status=fila[4],
                    )
                    conn.execute(insert_stmt)
                print(f"\n'{len(PEDIDOS_MULTIPLE_ROWS)}' row(s) inserted into '{self.table_name}' table!")
        elif self.table_name == "pedidos" and self.num_records > 0:
            with engine.begin() as conn:
                for _ in range(self.num_records):
                    insert_stmt = pedidos.insert().values(
                        id=int(
                            conn.execute(
                                pedidos.select().order_by(
                                    desc(pedidos.c.id)
                                )
                            ).first()[0]
                        ) + 1,
                        cliente_id=random.choice(
                            conn.execute(
                                clientes.select()
                            ).fetchall()
                        )[0],
                        fecha=fake.date_time().strftime("%Y/%m/%d %H:%M:%S %p"),
                        producto_id=random.choice(
                            conn.execute(
                                productos.select()
                            ).fetchall()
                        )[0],
                        status=fake.pybool(),
                    )
                    conn.execute(insert_stmt)
                print(f"\n'{self.num_records}' row(s) inserted into '{self.table_name}' table!")
        else:
            pass


if __name__ == "__main__":
    table_name = ""
    num_records = 0

    if len(sys.argv) == 2:
        if sys.argv[1]:
            table_name = sys.argv[1]
            num_records = 0

    if len(sys.argv) == 3:
        if sys.argv[1]:
            table_name = sys.argv[1]
        if int(sys.argv[2]) > 0:
            num_records = int(sys.argv[2])
        else:
            num_records = 0

    if table_name in ['estados', 'cuidades', 'categorias']:
        generate_data = GenerateData(table_name)
        generate_data.create_data()
    else:
        generate_data = GenerateData(table_name, num_records)
        generate_data.create_data()
