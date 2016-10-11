/*
   Copyright (C) 2009-2015 Red Hat, Inc.

   This library is free software; you can redistribute it and/or
   modify it under the terms of the GNU Lesser General Public
   License as published by the Free Software Foundation; either
   version 2.1 of the License, or (at your option) any later version.

   This library is distributed in the hope that it will be useful,
   but WITHOUT ANY WARRANTY; without even the implied warranty of
   MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU
   Lesser General Public License for more details.

   You should have received a copy of the GNU Lesser General Public
   License along with this library; if not, see <http://www.gnu.org/licenses/>.
*/
#ifdef HAVE_CONFIG_H
#include <config.h>
#endif

#include "dummy-channel-client.h"
#include "red-channel.h"

static void dummy_channel_client_initable_interface_init(GInitableIface *iface);

G_DEFINE_TYPE_WITH_CODE(DummyChannelClient, dummy_channel_client, RED_TYPE_CHANNEL_CLIENT,
                        G_IMPLEMENT_INTERFACE(G_TYPE_INITABLE,
                                              dummy_channel_client_initable_interface_init))

#define DUMMY_CHANNEL_CLIENT_PRIVATE(o) \
    (G_TYPE_INSTANCE_GET_PRIVATE((o), TYPE_DUMMY_CHANNEL_CLIENT, DummyChannelClientPrivate))

struct DummyChannelClientPrivate
{
    gboolean connected;
};

static int dummy_channel_client_pre_create_validate(RedChannel *channel, RedClient  *client)
{
    if (red_client_get_channel(client, channel->type, channel->id)) {
        spice_printerr("Error client %p: duplicate channel type %d id %d",
                       client, channel->type, channel->id);
        return FALSE;
    }
    return TRUE;
}

static gboolean dummy_channel_client_initable_init(GInitable *initable,
                                                   GCancellable *cancellable,
                                                   GError **error)
{
    GError *local_error = NULL;
    DummyChannelClient *self = DUMMY_CHANNEL_CLIENT(initable);
    RedChannelClient *rcc = RED_CHANNEL_CLIENT(self);
    RedClient *client = red_channel_client_get_client(rcc);
    RedChannel *channel = red_channel_client_get_channel(rcc);
    pthread_mutex_lock(&client->lock);
    if (!dummy_channel_client_pre_create_validate(channel,
                                                  client)) {
        g_set_error(&local_error,
                    SPICE_SERVER_ERROR,
                    SPICE_SERVER_ERROR_FAILED,
                    "Client %p: duplicate channel type %d id %d",
                    client, channel->type, channel->id);
        goto cleanup;
    }

    rcc->incoming.header.data = rcc->incoming.header_buf;

    red_channel_add_client(channel, rcc);
    red_client_add_channel(client, rcc);

cleanup:
    pthread_mutex_unlock(&client->lock);
    if (local_error) {
        g_warning("Failed to create channel client: %s", local_error->message);
        g_propagate_error(error, local_error);
    }
    return local_error == NULL;
}

static void dummy_channel_client_initable_interface_init(GInitableIface *iface)
{
    iface->init = dummy_channel_client_initable_init;
}

static gboolean dummy_channel_client_is_connected(RedChannelClient *rcc)
{
    return DUMMY_CHANNEL_CLIENT(rcc)->priv->connected;
}

static void dummy_channel_client_disconnect(RedChannelClient *rcc)
{
    DummyChannelClient *self = DUMMY_CHANNEL_CLIENT(rcc);
    RedChannel *channel = red_channel_client_get_channel(rcc);
    GList *link;

    if (channel && (link = g_list_find(channel->clients, rcc))) {
        spice_printerr("rcc=%p (channel=%p type=%d id=%d)", rcc, channel,
                       channel->type, channel->id);
        red_channel_remove_client(channel, link->data);
    }
    self->priv->connected = FALSE;
}

static void
dummy_channel_client_class_init(DummyChannelClientClass *klass)
{
    RedChannelClientClass *cc_class = RED_CHANNEL_CLIENT_CLASS(klass);

    g_type_class_add_private(klass, sizeof(DummyChannelClientPrivate));

    cc_class->is_connected = dummy_channel_client_is_connected;
    cc_class->disconnect = dummy_channel_client_disconnect;
}

static void
dummy_channel_client_init(DummyChannelClient *self)
{
    self->priv = DUMMY_CHANNEL_CLIENT_PRIVATE(self);

    self->priv->connected = TRUE;
}

RedChannelClient* dummy_channel_client_create(RedChannel *channel,
                                              RedClient  *client,
                                              int num_common_caps,
                                              uint32_t *common_caps,
                                              int num_caps, uint32_t *caps)
{
    RedChannelClient *rcc;
    GArray *common_caps_array = NULL, *caps_array = NULL;

    if (common_caps) {
        common_caps_array = g_array_sized_new(FALSE, FALSE, sizeof (*common_caps),
                                              num_common_caps);
        g_array_append_vals(common_caps_array, common_caps, num_common_caps);
    }
    if (caps) {
        caps_array = g_array_sized_new(FALSE, FALSE, sizeof (*caps), num_caps);
        g_array_append_vals(caps_array, caps, num_caps);
    }

    rcc = g_initable_new(TYPE_DUMMY_CHANNEL_CLIENT,
                         NULL, NULL,
                         "channel", channel,
                         "client", client,
                         "caps", caps_array,
                         "common-caps", common_caps_array,
                         NULL);

    if (caps_array)
        g_array_unref(caps_array);
    if (common_caps_array)
        g_array_unref(common_caps_array);

    return rcc;
}