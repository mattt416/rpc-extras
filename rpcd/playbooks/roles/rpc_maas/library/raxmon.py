#!/usr/bin/env python

import ConfigParser

from ansible.module_utils.basic import *


def _get_agent_tokens(conn, entity):
    agent_tokens = []
    for a in conn.list_agent_tokens():
        if a.label == entity:
            agent_tokens.append(a)
    return agent_tokens


def _get_conn(get_driver, Provider):
    cfg = ConfigParser.RawConfigParser()
    cfg.read('/root/.raxrc')
    driver = get_driver(Provider.RACKSPACE)
    conn = None
    try:
        user = cfg.get('credentials', 'username')
        api_key = cfg.get('credentials', 'api_key')
        conn = driver(user, api_key)
    except (ConfigParser.NoSectionError, ConfigParser.NoOptionError):
        url = cfg.get('api', 'url')
        token = cfg.get('api', 'token')
        conn = driver(None, None, ex_force_base_url=url,
                      ex_force_auth_token=token)
    return conn


def _get_entities(conn, entity):
    entities = []
    for e in conn.list_entities():
        if e.label == entity:
            entities.append(e)
    return entities


def assign_agent_to_entity(module, conn, entity):
    entities = _get_entities(conn, entity)
    entities_count = len(entities)
    if entities_count == 0:
        msg = "Zero entities with the label %s exist. Entities should be " \
              "created as part of the hardware provisioning process, if " \
              "missing, please consult the internal documentation for " \
              "associating one with the device." % (entity)
        module.fail_json(msg=msg)
    elif entities_count == 1:
        conn.update_entity(entities[0], {'agent_id': entity})
        module.exit_json(
            changed=True
        )
    elif entities_count > 1:
        msg = "Entity count of %s != 1 for entity with the label %s. Reduce " \
              "the entity count to one by deleting or re-labelling the " \
              "duplicate entities." % (entities_count, entity)
        module.fail_json(msg=msg)


def create_agent_token(module, conn, entity):
    agent_tokens = _get_agent_tokens(conn, entity)
    agent_tokens_count = len(agent_tokens)
    if agent_tokens_count == 0:
        module.exit_json(
            changed=True,
            id=conn.create_agent_token(label=entity).id
        )
    elif agent_tokens_count == 1:
        module.exit_json(
            changed=False,
            id=agent_tokens[0].id
        )
    elif agent_tokens_count > 1:
        msg = "Agent token count of %s > 1 for entity with " \
              "the label %s" % (agent_tokens_count, entity)
        module.fail_json(msg=msg)


def main():
    module = AnsibleModule(
        argument_spec = dict(
            cmd = dict(required=True),
            entity = dict(required=True),
            venv_bin = dict()
        )
    )

    if module.params['venv_bin']:
        activate_this = '%s/activate_this.py' % (module.params['venv_bin'])
        execfile(activate_this, dict(__file__=activate_this))

    from rackspace_monitoring.providers import get_driver
    from rackspace_monitoring.types import Provider

    conn = _get_conn(get_driver, Provider)

    if module.params['cmd'] == 'assign_agent_to_entity':
        assign_agent_to_entity(module, conn, module.params['entity'])
    elif module.params['cmd'] == 'create_agent_token':
        create_agent_token(module, conn, module.params['entity'])
    else:
        module.fail_json(msg='Command "%s" not valid' % (module.params['cmd']))


if __name__ == '__main__':
    main()
