from sqlalchemy import func, desc, select, and_

from conf.models import Grade, Group, Student, Subject, Teacher
from conf.db import session


def select_01():
    """
    SELECT 
    s.id, 
    s.fullname, 
    ROUND(AVG(g.grade), 2) AS average_grade
    FROM students s
    JOIN grades g ON s.id = g.student_id
    GROUP BY s.id
    ORDER BY average_grade DESC
    LIMIT 5;
    """

    result = session.query(Student.id, Student.fullname, func.round(func.avg(Grade.grade), 2).label('average_grade')) \
        .select_from(Student).join(Grade).group_by(Student.id).order_by(desc('average_grade')).limit(5).all()
    return result


def select_02():
    """
    SELECT 
    s.id, 
    s.fullname, 
    ROUND(AVG(g.grade), 2) AS average_grade
    FROM grades g
    JOIN students s ON s.id = g.student_id
    where g.subject_id = 1
    GROUP BY s.id
    ORDER BY average_grade DESC
    LIMIT 1;
    """

    result = session.query(Student.id, Student.fullname, func.round(func.avg(Grade.grade), 2).label('average_grade')) \
        .select_from(Grade).join(Student).filter(Grade.subjects_id == 1).group_by(Student.id) \
        .order_by(desc('average_grade')).limit(1).all()
    return result


def select_03():
    """
    SELECT sub.name as subject,
    g.name as group_name,
    ROUND(AVG(gr.grade), 2) as average_grade
    FROM   subjects sub
    JOIN   grades gr ON sub.id = gr.subject_id
    JOIN   students s ON s.id = gr.student_id
    JOIN   groups g ON g.id = s.group_id
    WHERE  sub.id = 4
    GROUP BY subject, group_name
    ORDER BY average_grade DESC;
    """

    result = session.query(Subject.name.label('subject'), Group.name.label('group_name') \
        , func.round(func.avg(Grade.grade), 2).label('average_grade')) \
        .select_from(Subject).join(Grade).join(Student).join(Group).filter(Subject.id == 4) \
        .group_by(Subject.name, Group.name).order_by(desc('average_grade')).all()
    return result


def select_04():
    """
    SELECT
    ROUND(AVG(grade), 2) as average_grade
    FROM grades;
    """

    result = session.query(func.round(func.avg(Grade.grade), 2).label('average_grade')).select_from(Grade).all()
    return result


def select_05():
    """
    SELECT
    t.fullname as teacher,
    s.name as subject_name
    FROM teachers t 
    JOIN subjects s on t.id = s.teacher_id
    WHERE t.id = 1;
    """

    result = session.query(Teacher.fullname.label('teacher'), Subject.name.label('subject_name')) \
        .select_from(Teacher).join(Subject).filter(Teacher.id == 1).all()
    return result


def select_06():
    """
    SELECT
    g.name as group_name,
    s.fullname as students_name
    FROM students s  
    JOIN groups g on g.id = s.group_id
    WHERE g.id = 2;
    """

    result = session.query(Group.name.label('group_name'), Student.fullname.label('students_name')) \
        .select_from(Student).join(Group).filter(Group.id == 2).all()
    return result


def select_07():
    """
    SELECT
    gr.grade as grade,
    s.fullname as students_name,
    g.name as group_name,
    su.name as subject
    FROM grades gr  
    JOIN subjects su on su.id = gr.subject_id
    JOIN students s on s.id = gr.student_id
    JOIN groups g on g.id = s.group_id
    WHERE su.id = 1 AND g.id = 1;
    """

    result = session.query(Grade.grade.label('grade'), Student.fullname.label('students_name') \
        , Group.name.label('group_name'), Subject.name.label('subject')) \
        .select_from(Grade).join(Subject).join(Student).filter(and_(Subject.id == 1, Group.id == 1)).all()
    return result


def select_08():
    """
    SELECT sub.name as subject,
    t.fullname as teacher,
    ROUND(AVG(g.grade), 2) as average_grade
    FROM teachers t
    JOIN subjects sub on t.id = sub.teacher_id
    JOIN grades g on sub.id = g.subject_id
    WHERE t.id = 1
    GROUP BY subject, teacher
    ORDER BY average_grade DESC;
    """

    result = session.query(Subject.name.label('subject'), Teacher.fullname.label('teacher_name') \
        , func.round(func.avg(Grade.grade), 2).label('average_grade')) \
        .select_from(Teacher).join(Subject).join(Grade).filter(Teacher.id == 1) \
        .group_by(Subject.name, Teacher.fullname).order_by(desc('average_grade')).all()
    return result


def select_09():
    """
    SELECT DISTINCT
    s.fullname AS student,
    sub.name AS subject       
    FROM students s
    JOIN grades g ON s.id = g.student_id
    JOIN subjects sub ON sub.id = g.subject_id
    WHERE s.id = 1;
    """

    result = session.query(func.distinct(Student.fullname.label('student'), Subject.name.label('subject'))) \
        .select_from(Student).join(Grade).join(Subject).filter(Student.id == 1).all()
    return result


def select_10():
    """
    SELECT DISTINCT
    s.fullname AS student,
    t.fullname AS teacher,
    sub.name AS subject       
    FROM students s
    JOIN grades g ON s.id = g.student_id
    JOIN subjects sub ON sub.id = g.subject_id
    JOIN teachers t ON t.id = sub.teacher_id
    WHERE s.id = 1 AND t.id = 1;
    """

    result = session.query(func.distinct(Student.fullname.label('student'), Teacher.fullname.label('teacher') \
        , Subject.name.label('subject'))) \
        .select_from(Student).join(Grade).join(Subject).join(Teacher).filter(and_(Student.id == 1, Teacher.id == 1)).all()
    return result


def select_11():
    """
    SELECT s.fullname as student,
        t.fullname as teacher,
        ROUND(AVG(g.grade), 2) as average_grade
    FROM teachers t
    JOIN subjects sub on t.id = sub.teacher_id
    JOIN grades g on sub.id = g.subject_id
    JOIN students s on s.id = g.student_id
    WHERE t.id = 1 AND s.id = 1
    GROUP BY student, teacher
    ORDER BY average_grade DESC;
    """

    result = session.query(Student.fullname.label('student'), Teacher.fullname.label('teacher') \
        , func.round(func.avg(Grade.grade), 2).label('average_grade')) \
        .select_from(Teacher).join(Subject).join(Grade).join(Student).filter(and_(Teacher.id == 1, Student.id == 1)) \
        .group_by(Student.fullname, Teacher.fullname).order_by(desc('average_grade')).all()
    return result


def select_12():
    """
    SELECT s.id, s.fullname, g.grade, g.grade_date
    FROM grades g
    JOIN students s ON g.student_id = s.id
    WHERE g.subject_id = 2 AND s.group_id = 3 AND g.grade_date = (
        SELECT MAX(grade_date)
        FROM grades g
        JOIN students s ON s.id=g.student_id
        WHERE g.subject_id = 2 AND s.group_id = 3
    );
    """

    subquery = (select(func.max(Grade.grade_date)).join(Student) \
        .filter(and_(Grade.subjects_id == 2, Student.group_id == 3))).scalar_subquery()

    result = session.query(Student.id, Student.fullname, Grade.grade, Grade.grade_date) \
        .select_from(Grade) \
        .join(Student) \
        .filter(and_(Grade.subjects_id == 2, Student.group_id == 3, Grade.grade_date == subquery)).all()

    return result


if __name__ == '__main__':
    print(select_01())
    print(select_02())
    print(select_03())      
    print(select_04())
    print(select_05())
    print(select_06())
    print(select_07())
    print(select_08())
    print(select_09())
    print(select_10())
    print(select_11())
    print(select_12())