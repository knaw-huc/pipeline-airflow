def get_step_names(context):
    current_step = context['task']
    previous_steps = [task for task in context['task'].upstream_list]
    return {
        "current_step": current_step,
        "previous_steps": previous_steps
    }