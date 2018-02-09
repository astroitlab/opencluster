import web

t_globals = dict(
  datestr=web.datestr,
)
render = web.template.render('templates/', cache=False, globals=t_globals)
render._keywords['globals']['render'] = render

def listing(**k):
    return render.listing()