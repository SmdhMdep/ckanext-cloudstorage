"use strict";

const GENERATE_HTML = `<i class="fa fa-cog"></i> Generate presigned URL`;
const GENERATING_HTML = `<i class="fa fa-spinner fa-pulse"></i> Generating presigned URL`;
const COPY_HTML = `<i class="fa fa-copy"></i> Copy presigned URL`;
const COPIED_HTML = `<i class="fa fa-check" style="color: #10ad64;"></i> Copied presigned URL valid for `;

const formatExpiry = expires_in => {
    const format = (amount, unit) => {
        if (amount < 1) return null;
        amount = Math.floor(amount);
        return `${amount} ${unit}${amount > 1 ? 's' : ''}`;
    }

    return (
        format(expires_in / (24 * 60 * 60), 'day')
        ?? format(expires_in / (60 * 60), 'hour')
        ?? format(expires_in / 60, 'minute')
        ?? format(expires_in, 'second')
    );
}

ckan.module('cloudstorage-presigned-url', function ($, _) {
    /**
     * Options:
     *   resource_id
     *   expires_in -- number of seconds until the presigned url expires.
     */

    return {
        // cached presigned url
        _presignedUrl: undefined,
        // timeout ID for transitioning the button from copied to copy state.
        _copiedTimeoutId: undefined,
        // used for cancelling the current copy animation.
        _cancellationToken: { cancel: () => { } },
        initialize: function () {
            $.proxyAll(this, /_on/);
            this.options.expires_in = Number(this.options.expires_in);

            this.el.popover({
                content: this._(`Copied presigned URL valid for ${formatExpiry(this.options.expires_in)}`),
                animation: false,
                placement: 'top',
                trigger: 'manual',
            });
            this.el.on('click', this._onClick);
            this._setUiState({ content: GENERATE_HTML, disabled: false });
        },
        _onClick: async function () {
            this._cancellationToken.cancel();
            if (!this._presignedUrl) {
                await this._doGenerate();
            }
            await this._doCopy(this._cancellationToken);
        },
        _doCopy: async function (cancellationToken) {
            await navigator.clipboard.writeText(this._presignedUrl);
            this._setUiState({ content: COPY_HTML, disabled: false });
            this.el.popover('show');
            await delay(1200, cancellationToken);
            this.el.popover('hide');
        },
        _doGenerate: async function () {
            try {
                this._setUiState({ content: GENERATING_HTML, disabled: true });
                this._presignedUrl = await this._generatePresignedUrl();
            } catch (e) {
                this._setUiState({ content: GENERATE_HTML, disabled: false });
                throw e;
            }
        },
        _setUiState: function ({ content, disabled }) {
            this.el.html(content);
            this.el.prop('disabled', disabled);
        },
        _generatePresignedUrl: async function () {
            const response = await this._callApi('resource_create_presigned_url', {
                'id': this.options.resource_id,
                'expires_in': Number(this.options.expires_in),
            });
            return response.result.url;
        },
        _callApi: function (path, data) {
            return new Promise((resolve, reject) => {
                this.sandbox.client.call('POST', path, data, resolve, reject);
            });
        },
    };
});

let delay = (ms, cancellationToken, resolution) => {
    return new Promise((resolve, reject) => {
        const timeoutId = setTimeout(resolve, ms, resolution);
        if (!cancellationToken) return;
        cancellationToken.cancel = () => {
            clearTimeout(timeoutId);
            cancellationToken.cancel = () => { };
            reject(new Error('cancelled'));
        };
    });
};
