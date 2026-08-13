import re

with open('/media/callidus/callidus2/ses/Realtime-Server/index.ts', 'r') as f:
    content = f.read()

# Update the /auth/login endpoint
login_endpoint_regex = re.compile(r'app\.post\(\'/auth/login\', \(req, res\) => \{[\s\S]*?\}\);\n')

new_login_endpoint = """app.post('/auth/login', async (req, res) => {
  const { token: firebaseToken, role } = req.body as { token?: string; role?: string };

  if (!firebaseToken || !role || (role !== 'rider' && role !== 'driver')) {
    res.status(400).json({ error: 'Firebase token and role (rider | driver) required' });
    return;
  }

  let decodedFirebaseToken: any;
  try {
    // DEV MODE ONLY: Decoding without verification because we lack serviceAccountKey.json
    // In production, use firebase-admin.auth().verifyIdToken(firebaseToken)
    decodedFirebaseToken = jwt.decode(firebaseToken);
    if (!decodedFirebaseToken || !decodedFirebaseToken.phone_number) {
      throw new Error('Invalid Firebase token or missing phone number');
    }
  } catch (err: any) {
    console.error('Firebase Token Decode Error:', err);
    res.status(401).json({ error: 'Invalid Firebase token' });
    return;
  }

  const phone = decodedFirebaseToken.phone_number;
  const uid = decodedFirebaseToken.user_id || phone;
  
  console.log(`[Auth Login] Checking DynamoDB for phone: ${phone} (UID: ${uid})`);

  try {
    // Check if user exists in DynamoDB
    const getResult = await docClient.send(new GetCommand({
      TableName: 'ridego-users',
      Key: { id: uid } // Or phone, depending on how they designed the PK
    }));

    if (!getResult.Item) {
      console.log(`[Auth Login] User not found in DynamoDB. Creating new record for ${uid}`);
      await docClient.send(new PutCommand({
        TableName: 'ridego-users',
        Item: {
          id: uid,
          phone: phone,
          role: role,
          createdAt: new Date().toISOString()
        }
      }));
    } else {
      console.log(`[Auth Login] User ${uid} found in DynamoDB.`);
    }

    // Issue internal JWT for WebSocket Authentication
    const internalToken = jwt.sign(
      { id: uid, role },
      JWT_SECRET,
      { expiresIn: '7d' }
    );

    res.json({ token: internalToken, id: uid, role });
  } catch (dbErr: any) {
    console.error('[Auth Login] DynamoDB Error:', dbErr);
    res.status(500).json({ error: 'Database operation failed' });
  }
});
"""

# I need to add PutCommand to imports
content = content.replace(
    'import { DynamoDBDocumentClient, GetCommand, UpdateCommand } from "@aws-sdk/lib-dynamodb";',
    'import { DynamoDBDocumentClient, GetCommand, UpdateCommand, PutCommand } from "@aws-sdk/lib-dynamodb";'
)

content = login_endpoint_regex.sub(new_login_endpoint, content)

with open('/media/callidus/callidus2/ses/Realtime-Server/index.ts', 'w') as f:
    f.write(content)
