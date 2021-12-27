from sqlalchemy import Column, Integer, String, Date, Float, ForeignKey, select, and_, DATETIME, DATE, exc
from sqlalchemy.orm import relationship
from config import Session, engine, base

ses = Session()
connection = None


class Users(base):
    __tablename__ = 'Users'
    userID = Column(Integer, primary_key=True, nullable=False)
    nickname = Column(String(50), nullable=False)
    status = Column(String(50), nullable=False)
    date_registration = Column(Date, nullable=False)

    def __init__(self, nickname, status, date_registration):
        self.nickname = nickname
        self.status = status
        self.date_registration = date_registration


    format_str = '{:^12}{:^40}{:^40}{:^30}'

    def __repr__(self):
        return self.format_str.format(self.userID, self.nickname, self.status, str(self.date_registration))

    def __attributes_print__(self):
        return self.format_str.format('userID', 'nickname', 'status', 'date_registration')


class Questions(base):
    __tablename__ = 'Questions'
    q_linkID = Column(Integer, primary_key=True, nullable=False)
    topic = Column(String(500), nullable=False)
    date = Column(Date, nullable=False)
    tags = Column(String(50), nullable=False)

    def __init__(self, topic, date, tags):
        self.tags = tags
        self.topic = topic
        self.date = date

    format_str = '{:^15}{:^100}{:^30}{:^40}'

    def __repr__(self):
        return self.format_str.format(self.q_linkID, self.topic, str(self.date), self.tags)

    def __attributes_print__(self):
        return self.format_str.format('q_linkID', 'topic', 'date', 'tags')


class Users_Questions(base):
    __tablename__ = 'Users/Questions'
    userID = Column(Integer, ForeignKey('Users.userID'), primary_key=True, nullable=False)
    q_linkID = Column(Integer, ForeignKey('Questions.q_linkID'), primary_key=True, nullable=False)
    Users = relationship('Users')
    Questions = relationship('Questions')

    format_str = '{:^12}{:^12}'

    def __repr__(self):
        return self.format_str.format(self.userID, self.q_linkID)

    def __attributes_print__(self):
        return self.format_str.format('userID', 'q_linkID')


class Answers(base):
    __tablename__ = 'Answers'
    a_linkID = Column(Integer, primary_key=True, nullable=False)
    date = Column(Date, nullable=False)
    fk_qlinkID = Column(Integer, ForeignKey('Questions.q_linkID'), nullable=False)
    answer = Column(String(500), nullable=False)
    pos_rating = Column(Integer, nullable=False)
    neg_rating = Column(Integer, nullable=False)

    Questions = relationship('Questions')

    def __init__(self, date, fk_qlinkID, answer, pos_rating, neg_rating):
        self.date = date
        self.fk_qlinkID = fk_qlinkID
        self.answer = answer
        self.pos_rating = pos_rating
        self.neg_rating = neg_rating


    format_str = '{:^12}{:^30}{:^15}{:^60}{:^12}{:^12}'

    def __repr__(self):

        return self.format_str.format(self.a_linkID, str(self.date), self.fk_qlinkID, self.answer, self.pos_rating,
                                      self.neg_rating)

    def __attributes_print__(self):
        return self.format_str.format('a_linkID', 'date', 'fk_qlinkID', 'answer', 'pos_rating', 'neg_rating')


def connect():
    # global connection = engine.connect()
    base.metadata.create_all(engine)


def insert(num: int, col: list) -> bool:
    if len(col) < 1:
        return False
    element = None
    try:
        match num:
            case 1:
                element = Users(*col)
            case 2:
                element = Questions(*col)
            case 4:
                element = Answers(*col)

        ses.add(element)
        ses.commit()
    except (Exception, exc.DBAPIError) as error:
        print("Can't insert into table", error)
        ses.rollback()
        return False
    return True


def myselect(num: int, quantity: int = 100, offset: int = 0, id: str = "") -> list:
    table = None
    primary_key = None
    if id:
        id = int(id)
    try:
        match num:
            case 1:
                if id:
                    return ses.query(Users).filter_by(userID=id).limit(quantity).all()
                else:
                    return ses.query(Users).order_by(Users.userID.asc()).offset(offset).limit(quantity).all()
            case 2:
                if id:
                    return ses.query(Questions).filter_by(q_linkID=id).limit(quantity).all()
                else:
                    return ses.query(Questions).order_by(Questions.q_linkID.asc()).offset(offset).limit(quantity).all()
            case 3:
                if id:
                    return ses.query(Users_Questions).filter_by(userID=id).limit(quantity).all()
                else:
                    return ses.query(Users_Questions).order_by(Users_Questions.userID.asc()).offset(offset).limit(
                        quantity).all()
            case 4:
                if id:
                    return ses.query(Answers).filter_by(a_linkID=id).limit(quantity).all()
                else:
                    return ses.query(Answers).order_by(Answers.a_linkID.asc()).offset(offset).limit(
                        quantity).all()

    except (Exception, exc.DBAPIError) as error:
        print("Can't select table", error)
        ses.rollback()
        return []


def delete(num: int, id: str) -> bool:
    if id:
        id = int(id)
    try:
        match num:
            case 1:
                ses.query(Users).filter_by(userID=int(id)).delete()
            case 2:
                ses.query(Questions).filter_by(q_linkID=id).delete()
            case 3:
                ses.query(Users_Questions).filter_by(id=id).delete()
            case 4:
                ses.query(Answers).filter_by(date=id).delete()

        ses.commit()
    except (Exception, exc.DBAPIError) as error:
        print("Can't delete from table", error)
        ses.rollback()
        return False
    return True


def update(num: int, col: list, id: str) -> bool:
    if id:
        id = int(id)
    try:
        match num:
            case 1:
                ses.query(Users).filter_by(userID=int(id)).update(
                    {Users.nickname: col[0], Users.status: col[1], Users.date_registration: col[2]})
            case 2:
                ses.query(Questions).filter_by(q_linkID=id).update(
                    {Questions.topic: col[0], Questions.date: col[1], Questions.tags: col[2]})
            case 3:
                ses.query(Users_Questions).filter_by(userID=id).update(
                    {Users_Questions.userID: col[0], Users_Questions.q_linkID: col[1]})
            case 4:
                ses.query(Answers).filter_by(a_linkID=int(id)).update(
                    {Answers.date: col[0], Answers.fk_qlinkID: col[1], Answers.answer: col[2],
                     Answers.pos_rating: col[3], Answers.neg_rating: col[4]})
        ses.commit()
    except (Exception, exc.DBAPIError) as error:
        print("Can't update table", error)
        ses.rollback()
        return False
    return True
