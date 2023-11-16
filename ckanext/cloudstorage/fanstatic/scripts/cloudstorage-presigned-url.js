"use strict";

const GENERATE_HTML = `<i class="fa fa-cog"></i> Generate presigned URL`;
const GENERATING_HTML = `<i class="fa fa-spinner fa-pulse"></i> Generating presigned URL`;
const COPY_HTML = `<i class="fa fa-copy"></i> Copy presigned URL`;
const COPIED_HTML = `<i class="fa fa-check" style="color: #10ad64;"></i> Copied presigned URL`;

ckan.module('cloudstorage-presigned-url', function ($, _) {
    /**
     * Options:
     *   resource_id
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

            this.el.popover({
                content: this._('URL copied'),
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
            this._setUiState({ content: COPIED_HTML, disabled: false });
            await delay(800, cancellationToken);
            this._setUiState({ content: COPY_HTML, disabled: false });
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
