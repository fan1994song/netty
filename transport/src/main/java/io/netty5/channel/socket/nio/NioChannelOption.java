/*
 * Copyright 2018 The Netty Project
 *
 * The Netty Project licenses this file to you under the Apache License,
 * version 2.0 (the "License"); you may not use this file except in compliance
 * with the License. You may obtain a copy of the License at:
 *
 *   https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations
 * under the License.
 */
package io.netty5.channel.socket.nio;

import io.netty5.channel.ChannelException;
import io.netty5.channel.ChannelOption;

import java.io.IOException;
import java.nio.channels.NetworkChannel;
import java.nio.channels.ServerSocketChannel;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;

/**
 * Provides {@link ChannelOption} over a given {@link java.net.SocketOption} which is then passed through the underlying
 * {@link java.nio.channels.NetworkChannel}.
 */
public final class NioChannelOption<T> extends ChannelOption<T> {

    private final java.net.SocketOption<T> option;

    @SuppressWarnings("deprecation")
    private NioChannelOption(java.net.SocketOption<T> option) {
        super(option.name());
        this.option = option;
    }

    /**
     * Returns a {@link ChannelOption} for the given {@link java.net.SocketOption}.
     */
    public static <T> ChannelOption<T> of(java.net.SocketOption<T> option) {
        return new NioChannelOption<>(option);
    }

    // Internal helper methods to remove code duplication between Nio*Channel implementations.
    static <T> void setOption(NetworkChannel channel, NioChannelOption<T> option, T value) {
        if (channel instanceof ServerSocketChannel && option.option == java.net.StandardSocketOptions.IP_TOS) {
            // Skip IP_TOS as a workaround for a JDK bug:
            // See https://mail.openjdk.java.net/pipermail/nio-dev/2018-August/005365.html
            return;
        }
        try {
            channel.setOption(option.option, value);
        } catch (IOException e) {
            throw new ChannelException(e);
        }
    }

    static <T> T getOption(NetworkChannel channel, NioChannelOption<T> option) {
        if (channel instanceof ServerSocketChannel && option.option == java.net.StandardSocketOptions.IP_TOS) {
            // Skip IP_TOS as a workaround for a JDK bug:
            // See https://mail.openjdk.java.net/pipermail/nio-dev/2018-August/005365.html
            return null;
        }
        try {
            return channel.getOption(option.option);
        } catch (IOException e) {
            throw new ChannelException(e);
        }
    }

    static boolean isSupported(NetworkChannel channel, NioChannelOption<?> option) {
        return channel.supportedOptions().contains(option.option);
    }

    @SuppressWarnings("unchecked")
    static ChannelOption[] getOptions(NetworkChannel channel) {
        Set<java.net.SocketOption<?>> supportedOpts = channel.supportedOptions();

        if (channel instanceof ServerSocketChannel) {
            List<ChannelOption<?>> extraOpts = new ArrayList<>(supportedOpts.size());
            for (java.net.SocketOption<?> opt : supportedOpts) {
                if (opt == java.net.StandardSocketOptions.IP_TOS) {
                    // Skip IP_TOS as a workaround for a JDK bug:
                    // See https://mail.openjdk.java.net/pipermail/nio-dev/2018-August/005365.html
                    continue;
                }
                extraOpts.add(new NioChannelOption(opt));
            }
            return extraOpts.toArray(new ChannelOption[0]);
        } else {
            ChannelOption<?>[] extraOpts = new ChannelOption[supportedOpts.size()];

            int i = 0;
            for (java.net.SocketOption<?> opt : supportedOpts) {
                extraOpts[i++] = new NioChannelOption(opt);
            }
            return extraOpts;
        }
    }
}
