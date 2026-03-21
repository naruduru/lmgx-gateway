# lmgx-studio-web-editor 분리 안내

요청사항에 따라 `lmgx-studio-web-editor`는 `lmgx-gateway` 내부 서브폴더가 아니라,
완전히 분리된 독립 Git 리포지토리로 생성되었습니다.

- 새 로컬 경로: `/workspace/lmgx-studio-web-editor`
- 현재 초기 커밋: `45fb5a9`
- 현재 기본 브랜치: `main`

## 원격 저장소 연결 / Push 예시

```bash
cd /workspace/lmgx-studio-web-editor
git remote add origin <YOUR_NEW_REPO_URL>
git push -u origin main
```

## 참고

`lmgx-gateway`에는 웹 에디터 소스를 포함하지 않고,
웹 에디터 작업은 분리된 저장소에서 진행합니다.
