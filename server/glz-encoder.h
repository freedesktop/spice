/*
   Copyright (C) 2009 Red Hat, Inc.

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

#ifndef GLZ_ENCODER_H_
#define GLZ_ENCODER_H_

/* Manging the lz encoding using a dictionary that is shared among encoders */

#include <common/lz_common.h>

#include "red-common.h"
#include "glz-encoder-dict.h"

SPICE_BEGIN_DECLS

struct GlzEncoderUsrContext {
    SPICE_GNUC_PRINTF(2, 3) void (*error)(GlzEncoderUsrContext *usr, const char *fmt, ...);
    SPICE_GNUC_PRINTF(2, 3) void (*warn)(GlzEncoderUsrContext *usr, const char *fmt, ...);
    SPICE_GNUC_PRINTF(2, 3) void (*info)(GlzEncoderUsrContext *usr, const char *fmt, ...);
    void    *(*malloc)(GlzEncoderUsrContext *usr, int size);
    void (*free)(GlzEncoderUsrContext *usr, void *ptr);

    // get the next chunk of the image which is entered to the dictionary. If the image is down to
    // top, return it from the last line to the first one (stride should always be positive)
    int (*more_lines)(GlzEncoderUsrContext *usr, uint8_t **lines);

    // get the next chunk of the compressed buffer.return number of bytes in the chunk.
    int (*more_space)(GlzEncoderUsrContext *usr, uint8_t **io_ptr);

    // called when an image is removed from the dictionary, due to the window size limit
    void (*free_image)(GlzEncoderUsrContext *usr, GlzUsrImageContext *image);

};

typedef void GlzEncoderContext;

GlzEncoderContext *glz_encoder_create(uint8_t id, GlzEncDictContext *dictionary,
                                      GlzEncoderUsrContext *usr);

void glz_encoder_destroy(GlzEncoderContext *opaque_encoder);

/*
        assumes width is in pixels and stride is in bytes
    usr_context       : when an image is released from the window due to capacity overflow,
                        usr_context is given as a parameter to the free_image callback.
    o_enc_dict_context: if glz_enc_dictionary_remove_image is called, it should be
                        called with the o_enc_dict_context that is associated with
                        the image.

        return: the number of bytes in the compressed data and sets o_enc_dict_context

        NOTE  : currently supports only rgb images in which width*bytes_per_pixel = stride OR
                palette images in which stride equals the min number of bytes to hold a line.
                The stride should be > 0
*/
int glz_encode(GlzEncoderContext *opaque_encoder, LzImageType type, int width, int height,
               int top_down, uint8_t *lines, unsigned int num_lines, int stride,
               uint8_t *io_ptr, unsigned int num_io_bytes, GlzUsrImageContext *usr_context,
               GlzEncDictImageContext **o_enc_dict_context);

SPICE_END_DECLS

#endif /* GLZ_ENCODER_H_ */
