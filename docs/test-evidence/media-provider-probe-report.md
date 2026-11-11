# Media Provider Probe Report

## Scope

Repository: `/Users/wujingvijay/workdir/ai-tool`

The probe used the existing local Provider adapters and configuration. Output
URLs, credentials and raw Provider payloads were intentionally not recorded.

## Successful probes

| Kind | Model | Adapter | Result | Elapsed |
| --- | --- | --- | --- | ---: |
| image | `seedream-5.0` | `common.image.providers.ark.ArkProvider` | image result returned | 10.9s |
| video | `sed2` | `common.video.providers.ark.ArkVideoProvider` | video result returned; external task observed | 207.9s |
| audio | `speech-2.8-turbo` | `common.audio.providers.minimax.MiniMaxAudioProvider` | audio result returned | 10.8s |

## Catalog inventory

The local catalog contains image models from Bilibili/Banana, Qwen, Ark/
SeeDream, Midjourney, Wanx and OpenAI Image; video models from Ark/Seedance,
PPIO/Kling/Wan/Veo, HappyHorse, Gemini Omni, MiniMax and Vidu; and audio/music
models from MiniMax T2A and music generation.

Only the three successful probes above are approved for the first remote
catalog. Every additional model needs an isolated minimal request and output
validation before enabling it.

## Safety

No key, credential-bearing URL, signed result URL or raw prompt is stored in
this report. Remote deployment configuration must use ignored environment files
or the service secret store, never committed source or fixtures.
