# Using scheduleFire with Django

scheduleFire has no Django-specific code. Django is a WSGI app, and jobs are ordinary functions.

## 1. Configure storage once, in an `AppConfig`

```python
# myapp/apps.py
from django.apps import AppConfig
from django.conf import settings

class MyAppConfig(AppConfig):
    name = "myapp"

    def ready(self):
        import schedulefire
        schedulefire.configure(storage=schedulefire.SQLiteStorage(settings.BASE_DIR / "jobs.db"))
```

## 2. Write jobs as module-level functions that take IDs, not model instances

```python
# myapp/jobs.py
from django.core.mail import send_mail
from schedulefire import AutomaticRetry

@AutomaticRetry(attempts=5)
def send_welcome_email(user_id: int) -> None:
    from django.contrib.auth.models import User
    user = User.objects.get(pk=user_id)
    send_mail("Welcome", "Hello!", None, [user.email])
```

## 3. Enqueue from views

```python
# myapp/views.py
from django.db import transaction
from schedulefire import BackgroundJob
from . import jobs

def signup(request):
    user = ...  # create the user
    # Enqueue after the DB commit so the worker can see the new row.
    transaction.on_commit(lambda: BackgroundJob.enqueue(jobs.send_welcome_email, user.pk))
```

## 4. Mount the dashboard in `wsgi.py`

```python
# mysite/wsgi.py
import os
from django.core.wsgi import get_wsgi_application
from schedulefire.dashboard import mount_dashboard

os.environ.setdefault("DJANGO_SETTINGS_MODULE", "mysite.settings")
application = mount_dashboard(get_wsgi_application())   # -> /schedule-fire; add auth=BasicAuth(...) or auth=OAuth(...)
```

`manage.py runserver` doesn't use `wsgi.py`. During development, run the dashboard on
its own port instead: `python -m schedulefire dashboard --db jobs.db`.

## 5. Run workers with Django set up

Jobs use the ORM, so the worker process must call `django.setup()` first:

```python
# myapp/management/commands/schedulefire_server.py
from django.core.management.base import BaseCommand
from schedulefire import BackgroundJobServer

class Command(BaseCommand):
    help = "Run scheduleFire background job workers"

    def handle(self, *args, **options):
        BackgroundJobServer(queues=["critical", "default"]).run()
```

Start the workers with `python manage.py schedulefire_server`.
