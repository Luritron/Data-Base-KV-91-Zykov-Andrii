# from model import connect, disconnect
import model as db
from controller import main_select_option

db.connect()

main_select_option()

db.disconnect()

