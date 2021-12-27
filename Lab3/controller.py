import model as db


def select_table(insert=False) -> int:
    if insert:
        print('Select the table to insert data, from 0-6')
    else:
        print('Select table, from 0-6')
    print('1. Users\n2. Questions\n3. Users/Questions\n4. Answers\n0. Return to main menu')
    num = int(input())
    if num > 6:
        print('Incorrect number')
        select_table()
    return num


def input_values(num: int) -> list[str]:
    print("Insert value seperated by comma")
    values = ""
    match num:
        case 1:
            values = input('Input: nickname->char[50], status->char[20], date_registration->date\n')
        case 2:
            values = input('Input: topic->char[500], date->date, tags->char[50]\n')
        case 4:
            values = input('Input: date->date, fk_qlinkID->integer, answer->char[500],pos_rating->integer, neg_rating->integer\n')

    return values.split(',')


def insert_option(num: int):
    if num == 0:
        return
    # if num == 4:
    #     print("Can't insert in this table")
    #     return
    columns = [column.strip() for column in input_values(num)]
    if db.insert(num, columns):
        print("Inserted successfully")
    else:
        print("Can't insert")



def print_option(num: int, id: str = "", quantity: int = 0, offset: int = 0) -> str:
    if num == 0:
        return
    if not id:
        if not quantity:
            quantity = int(input('Input quantity of rows to print: '))
        rows = db.myselect(num, quantity, offset)
    else:
        rows = db.myselect(num, id=id)
    if len(rows) > 0:
        print(rows[0].__attributes_print__())
        for i in rows:
            print(i)

def delete_option(num: int):
    if num == 0:
        return
    id = input("Enter id of row you want to delete\n'p' -> print rows\n'r' -> return to menu\n")
    if id == 'r':
        return
    elif id == 'p':
        offset = 0
        while True:
            print(print_option(num, quantity=10, offset=offset))
            id = input(
                "Enter id of row you want to delete\n'n' -> next 10 rows\n'b' -> previous 10 rows\n'r' -> return to "
                "menu\n")
            if id == 'r':
                return
            elif id == 'n':
                offset += 10
            elif id == 'b':
                offset -= 10
            else:
                break
    # id = input
    if db.delete(num, id):
        print('Deleted successfully')
    else:
        print("Can't delete")


def edit_option(num: int):
    if num == 0:
        return
    id = input("Enter id of row you want to change\n'p' -> print rows\n'r' -> return to menu\n")
    if id == 'r':
        return
    elif id == 'p':
        offset = 0
        while True:
            print(print_option(num, quantity=10, offset=offset))
            id = input(
                "Enter id of row you want to change\n'n' -> next 10 rows\n'b' -> previous 10 rows\n'r' -> return to menu\n")
            if id == 'r':
                return
            elif id == 'n':
                offset += 10
            elif id == 'b':
                offset -= 10
            else:
                break
    print_option(num, id)
    print("If you don't want to change column -> write as it was")
    columns = input_values(num)
    print('Updated successfully') if db.update(num, columns, int(id)) else print("Can't update")


def main_select_option():
    while True:
        print('1. Insert data in table')
        print('2. Edit data in table')
        print('3. Delete data from table')
        print('4. Print rows')
        print('0. Exit')

        match input('\tSelect option 0-6: '):
            case '1':
                insert_option(select_table(True))
            case '2':
                edit_option(select_table())
            case '3':
                delete_option(select_table())
            case '4':
                print_option(select_table())
            case '0':
                return
