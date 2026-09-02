import pytest

from fb_monitor.photo_actor import parse_photo_actor_output, validate_photo_actor_input


def cdn(name: str, host: str = "scontent-tpe1-1.xx.fbcdn.net") -> str:
    return f"https://{host}/v/t39.30808-6/{name}.jpg?oh=temporary&oe=123"


@pytest.mark.parametrize(
    "payload",
    [
        {"cookies": [{"name": "c_user", "value": "123"}]},
        {"headers": {"Cookie": "c_user=123; xs=secret"}},
        {"options": {"access_token": "secret"}},
        {"headers": ["Authorization: Bearer secret"]},
    ],
)
def test_photo_actor_input_rejects_login_credentials(payload):
    with pytest.raises(ValueError):
        validate_photo_actor_input(payload)


def test_photo_actor_input_allows_pagination_tokens_and_urls():
    validate_photo_actor_input(
        {
            "urls": ["https://www.facebook.com/100"],
            "continuationUrl": "https://example.test/page?cursorToken=opaque",
            "nextToken": "opaque-page-token",
            "continuationToken": "opaque-continuation-token",
            "pagination": {"token": "opaque-scoped-page-token"},
            "options": {"maxItems": 100},
        }
    )


@pytest.mark.parametrize(
    "key",
    [
        "facebookAccessToken",
        "AuthorizationHeader",
        "xFbToken",
        "credential",
        "serviceCredentials",
    ],
)
def test_photo_actor_input_rejects_auth_key_without_leaking_value(key):
    secret = "must-not-appear-in-error"
    with pytest.raises(ValueError) as raised:
        validate_photo_actor_input({"options": {key: secret}})

    assert str(raised.value) == f"$.options.{key}"
    assert secret not in str(raised.value)


@pytest.mark.parametrize(
    "query_key",
    [
        "access_token",
        "auth_token",
        "token",
        "cookie",
        "session",
        "session_id",
        "password",
        "c_user",
        "xs",
    ],
)
def test_photo_actor_input_rejects_auth_in_url_query_without_leaking_value(
    query_key,
):
    secret = "must-not-appear-in-error"
    with pytest.raises(ValueError) as raised:
        validate_photo_actor_input(
            {"urls": [f"https://www.facebook.com/100?{query_key}={secret}"]}
        )

    assert str(raised.value) == "$.urls.[0]"
    assert secret not in str(raised.value)


def test_profile_actor_string_photos_are_normalized_but_not_claimed_complete():
    result = parse_photo_actor_output(
        [
            {
                "url": "https://www.facebook.com/example",
                "name": "Example",
                "photos": [cdn("111_aaa"), cdn("222_bbb")],
                "timestamp": "2026-09-02T00:00:00Z",
            }
        ]
    )

    assert [item["image"] for item in result.items] == [cdn("111_aaa"), cdn("222_bbb")]
    assert result.completed is False
    assert result.next_cursor is None
    assert result.declared_total is None


def test_nested_photo_schema_keeps_permalink_caption_and_timestamp():
    result = parse_photo_actor_output(
        {
            "results": {
                "items": [
                    {
                        "photoId": "123",
                        "permalinkUrl": "https://www.facebook.com/photo.php?fbid=123&id=9",
                        "media": {
                            "images": [
                                {"src": cdn("123_small"), "width": 320, "height": 320},
                                {"src": cdn("123_large"), "width": 1600, "height": 1200},
                            ]
                        },
                        "caption": "hello",
                        "createdAt": "2026-08-01T01:02:03Z",
                    }
                ]
            }
        }
    )

    assert result.items == [
        {
            "id": "123",
            "url": "https://www.facebook.com/photo.php?fbid=123&id=9",
            "image": cdn("123_large"),
            "caption": "hello",
            "timestamp": "2026-08-01T01:02:03Z",
        }
    ]


def test_duplicate_photo_with_different_cdn_hosts_and_queries_is_merged():
    first = cdn("555_same", "scontent-a.xx.fbcdn.net")
    second = cdn("555_same", "scontent-b.xx.fbcdn.net").replace("temporary", "changed")
    result = parse_photo_actor_output(
        [
            {"photos": [first]},
            {
                "photos": [
                    {
                        "id": "555",
                        "url": "https://www.facebook.com/photo/?fbid=555",
                        "imageUrl": second,
                        "caption": "richer",
                    }
                ]
            },
        ]
    )

    assert len(result.items) == 1
    assert result.items[0]["url"] == "https://www.facebook.com/photo/?fbid=555"
    assert result.items[0]["caption"] == "richer"
    assert result.raw_candidate_count >= 2


def test_non_facebook_and_non_fbcdn_media_are_rejected():
    result = parse_photo_actor_output(
        [
            {
                "photos": [
                    "https://example.com/photo.jpg",
                    {"imageUrl": "https://evil.example/image.jpg"},
                    {"url": "javascript:alert(1)"},
                ]
            }
        ]
    )

    assert result.items == []


def test_explicit_end_of_pagination_is_required_for_completed():
    image = cdn("777_photo")
    without_terminal = parse_photo_actor_output(
        [{"photos": [image], "totalPhotos": 1}]
    )
    terminal = parse_photo_actor_output(
        [
            {
                "photos": [image],
                "pagination": {"hasNextPage": False, "nextCursor": None},
                "totalPhotos": 1,
            }
        ]
    )

    assert without_terminal.completed is False
    assert terminal.completed is True
    assert terminal.has_next_page is False
    assert terminal.declared_total == 1


def test_cursor_or_declared_missing_items_overrides_terminal_signal():
    cursor_result = parse_photo_actor_output(
        [{"photos": [cdn("1_one")], "pageInfo": {"hasNextPage": True, "endCursor": "abc"}}]
    )
    missing_result = parse_photo_actor_output(
        [
            {
                "photos": [cdn("1_one")],
                "totalPhotos": 2,
                "pagination": {"hasNextPage": False},
            }
        ]
    )

    assert cursor_result.completed is False
    assert cursor_result.next_cursor == "abc"
    assert cursor_result.has_next_page is True
    assert missing_result.completed is False
    assert missing_result.declared_total == 2


def test_summary_terminal_evidence_can_complete_an_inventory():
    result = parse_photo_actor_output(
        [{"photos": [cdn("999_photo")]}],
        {"coverageStatus": "COMPLETE", "pagination": {"hasMore": False}},
    )

    assert result.completed is True
    assert result.terminal_reason == "COMPLETE"


def test_actor_run_succeeded_is_not_inventory_terminal_evidence():
    result = parse_photo_actor_output(
        [{"photos": [cdn("999_photo")]}],
        {"status": "SUCCEEDED"},
    )

    assert result.completed is False


def test_flat_photo_envelope_count_prevents_false_completion():
    result = parse_photo_actor_output(
        [
            {
                "id": "one",
                "imageUrl": cdn("one"),
                "totalPhotos": 2,
                "pagination": {"hasNextPage": False},
            }
        ]
    )

    assert len(result.items) == 1
    assert result.declared_total == 2
    assert result.completed is False


def test_profile_header_and_other_owner_photos_are_rejected():
    result = parse_photo_actor_output(
        [
            {
                "name": "Profile header",
                "image": cdn("avatar"),
                "photos": [
                    {
                        "id": "own",
                        "imageUrl": cdn("own"),
                        "url": "https://www.facebook.com/photo.php?fbid=10&id=100",
                    },
                    {
                        "id": "other",
                        "imageUrl": cdn("other"),
                        "url": "https://www.facebook.com/photo.php?fbid=20&id=999",
                    },
                ],
            }
        ],
        target_profile_id="100",
    )

    assert [item["id"] for item in result.items] == ["own"]


@pytest.mark.parametrize("identity_key", ["id", "profileId", "facebookId", "userId"])
def test_target_bound_cdn_photos_require_matching_root_profile_id(identity_key):
    result = parse_photo_actor_output(
        {identity_key: "100", "photos": [cdn("verified")]},
        target_profile_id="100",
    )

    assert [item["image"] for item in result.items] == [cdn("verified")]


@pytest.mark.parametrize(
    "identity_key,identity_url",
    [
        ("profileUrl", "https://www.facebook.com/100"),
        ("facebookUrl", "https://www.facebook.com/profile.php?id=100"),
        ("pageUrl", "https://www.facebook.com/people/Example/100/"),
        ("url", "https://www.facebook.com/100"),
    ],
)
def test_target_bound_cdn_photos_accept_verified_root_profile_url(
    identity_key, identity_url
):
    result = parse_photo_actor_output(
        {identity_key: identity_url, "photos": [cdn("verified-url")]},
        target_profile_id="100",
    )

    assert [item["image"] for item in result.items] == [cdn("verified-url")]


def test_target_bound_cdn_photos_reject_wrong_or_missing_envelope_identity():
    wrong = parse_photo_actor_output(
        {
            "profileId": "999",
            "photos": [cdn("wrong-profile")],
            "pagination": {"hasNextPage": False},
        },
        target_profile_id="100",
    )
    missing = parse_photo_actor_output(
        {
            "photos": [cdn("missing-profile")],
            "pagination": {"hasNextPage": False},
        },
        target_profile_id="100",
    )

    assert wrong.items == []
    assert wrong.completed is False
    assert missing.items == []
    assert missing.completed is False


def test_matching_profile_envelope_binds_completion_evidence():
    result = parse_photo_actor_output(
        {
            "profileId": "100",
            "photos": [cdn("bound-complete")],
            "pagination": {"hasNextPage": False},
        },
        target_profile_id="100",
    )

    assert len(result.items) == 1
    assert result.completed is True


@pytest.mark.parametrize(
    "content_url",
    [
        "https://www.facebook.com/photo.php?fbid=123&id=100",
        "https://www.facebook.com/100/posts/123",
        "https://www.facebook.com/people/Example/100/posts/123",
        "https://www.facebook.com/permalink.php?story_fbid=123&id=100",
    ],
)
def test_content_permalink_is_not_profile_envelope_identity(content_url):
    result = parse_photo_actor_output(
        {"url": content_url, "photos": [cdn("unbound")]},
        target_profile_id="100",
    )

    assert result.items == []


def test_candidate_permalink_owner_overrides_profile_envelope_identity():
    result = parse_photo_actor_output(
        {
            "profileId": "999",
            "photos": [
                {
                    "id": "target-photo",
                    "imageUrl": cdn("target-photo"),
                    "url": "https://www.facebook.com/photo.php?fbid=1&id=100",
                },
                {
                    "id": "other-photo",
                    "imageUrl": cdn("other-photo"),
                    "url": "https://www.facebook.com/photo.php?fbid=2&id=999",
                },
            ],
        },
        target_profile_id="100",
    )

    assert [item["id"] for item in result.items] == ["target-photo"]


def test_nested_profile_envelope_does_not_import_avatar_as_photo():
    result = parse_photo_actor_output(
        {
            "data": {
                "name": "Nested profile header",
                "image": cdn("avatar"),
                "photos": [cdn("real-photo")],
            }
        }
    )

    assert [item["image"] for item in result.items] == [cdn("real-photo")]


def test_generic_data_container_is_not_photo_collection_evidence():
    result = parse_photo_actor_output(
        {"data": {"avatar": {"imageUrl": cdn("avatar-only")}}}
    )

    assert result.items == []
