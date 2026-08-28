// THEME 임베딩 (1024차원은 Bedrock 임베딩 모델에 결합된 값)
// GraphRAG 조회 경로: db.index.vector.queryNodes('theme_embedding', k, $qvec) → BELONGS_TO 1-hop 확장
CREATE VECTOR INDEX theme_embedding IF NOT EXISTS
FOR (t:Theme) ON t.embedding
OPTIONS {indexConfig: {
    `vector.dimensions`: 1024,
    `vector.similarity_function`: 'cosine'
}};

// BELONGS_TO 편입 사유 임베딩
CREATE VECTOR INDEX belongs_to_reason_embedding IF NOT EXISTS
FOR ()-[r:BELONGS_TO]-() ON r.reason_embedding
OPTIONS {indexConfig: {
    `vector.dimensions`: 1024,
    `vector.similarity_function`: 'cosine'
}};
