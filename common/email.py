
import smtplib
from config import read_config

def get_mail_pw():
    conn_data = read_config()
    return conn_data["Gmail"]["password"]

def enviar_ok(context):
    password = get_mail_pw()
    try:
        x = smtplib.SMTP('smtp.gmail.com', 587)
        x.starttls()
        x.login('lorenzoalliot@gmail.com', password)
        subject =  f'Airflow reporte {context["dag"]}{context["ds"]}'
        body_text = f'Tarea {context["task_instance_key_str"]} ejecutada exitosamente'
        message = 'Subject: {}\n\n{}'.format(subject, body_text)
        x.sendmail('lorenzoalliot@gmail.com', 'lorenzoalliot@gmail.com', message)
        print('Exito')
    except Exception as exception:
        print(exception)
        print('Failure')

def enviar_no (context):
    password = get_mail_pw()
    try:
        x=smtplib.SMTP('smtp.gmail.com',587)
        x.starttls()
        x.login('lorenzoalliot@gmail.com',password)
        subject=f'Airflow reprte {context["dag"]}{context["ds"]}'
        body_text= f'Tarea {context["task_instance_key_str"]} se ejecutó con errores\n\nError: {context.get("exception")}'
        message='Subject: {}\n\n{}'.format(subject,body_text)
        x.sendmail('lorenzoalliot@gmail.com','lorenzoalliot@gmail.com',message)
        print('Exito')
    except Exception as exception:
        print(exception)
        print('Failure')
